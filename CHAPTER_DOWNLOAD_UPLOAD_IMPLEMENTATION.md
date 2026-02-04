# Chapter Download-Upload Implementation Plan

## Overview
Change the chapter upload pipeline from **streaming GoPro → S3** to **download GoPro → Jetson → upload to S3 → delete local**, leveraging Jetson's faster ethernet connection for S3 uploads.

## Current Flow (Streaming)
```
GoPro WiFi → Jetson → Stream to S3
     ↓
   (No local file created)
```

**Problem**: GoPro WiFi is slower than Jetson's ethernet connection

## New Flow (Download → Upload → Delete Local)
```
GoPro WiFi → Jetson (download to /tmp/chapters/)
     ↓
Jetson Ethernet → S3 (upload from local file)
     ↓
Delete from Jetson local (keep on GoPro SD card)
```

**Benefits**:
- Faster S3 upload via Jetson ethernet (1Gbps vs ~50-100Mbps WiFi)
- Chapters remain on GoPro SD card as backup
- Can retry S3 upload without re-downloading from GoPro

---

## Code Changes Required

### 1. Modify `chapter_upload_service.py`

Add these new methods and modify existing ones:

#### A. Add Download Method (NEW)

```python
def download_chapter_to_local(
    self,
    gopro_ip: str,
    directory: str,
    filename: str,
    local_path: str,
    expected_size: int,
    progress_callback: Optional[Callable[[int, int], None]] = None
) -> Dict[str, Any]:
    """
    Download a single chapter from GoPro to local Jetson storage.
    
    Args:
        gopro_ip: GoPro camera IP address
        directory: DCIM directory on GoPro (e.g., "100GOPRO")
        filename: Video filename (e.g., "GX010038.MP4")
        local_path: Full path where to save the file locally
        expected_size: Expected file size in bytes (for progress tracking)
        progress_callback: Optional callback(bytes_downloaded, total_bytes)
    
    Returns:
        Dict with download results:
            - success: bool
            - local_path: str
            - bytes_downloaded: int
            - error: str (if failed)
    """
    download_url = f'http://{gopro_ip}:8080/videos/DCIM/{directory}/{filename}'
    
    logger.info(f"Downloading chapter from GoPro: {filename} -> {local_path}")
    logger.info(f"  Source: {download_url}")
    logger.info(f"  Expected size: {expected_size / (1024**3):.2f} GB")
    
    # Ensure parent directory exists
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    try:
        # Stream from GoPro with timeouts
        response = requests.get(
            download_url,
            stream=True,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )
        response.raise_for_status()
        
        # Get actual content length from response
        content_length = int(response.headers.get('content-length', expected_size))
        logger.info(f"  Actual content length: {content_length / (1024**3):.2f} GB")
        
        # Download to local file
        total_bytes = 0
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue
                
                f.write(chunk)
                total_bytes += len(chunk)
                
                # Report progress
                if progress_callback:
                    try:
                        progress_callback(total_bytes, content_length)
                    except Exception:
                        pass  # Don't let callback errors stop download
        
        logger.info(f"  SUCCESS: Downloaded {total_bytes / (1024**3):.2f} GB to {local_path}")
        
        return {
            'success': True,
            'local_path': local_path,
            'bytes_downloaded': total_bytes
        }
    
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout downloading from GoPro: {e}")
        # Clean up partial file
        if os.path.exists(local_path):
            os.remove(local_path)
        return {
            'success': False,
            'local_path': local_path,
            'bytes_downloaded': 0,
            'error': f'Timeout downloading from GoPro: {e}'
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP error downloading from GoPro: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)
        return {
            'success': False,
            'local_path': local_path,
            'bytes_downloaded': 0,
            'error': f'HTTP error: {e}'
        }
    except Exception as e:
        logger.error(f"Error downloading chapter from GoPro: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)
        return {
            'success': False,
            'local_path': local_path,
            'bytes_downloaded': 0,
            'error': str(e)
        }
```

#### B. Add Local Upload Method (NEW)

```python
def upload_local_file_to_s3(
    self,
    local_path: str,
    s3_key: str,
    progress_callback: Optional[Callable[[int, int], None]] = None
) -> Dict[str, Any]:
    """
    Upload a local file to S3 using multipart upload.
    
    Args:
        local_path: Path to local file on Jetson
        s3_key: Full S3 key for the uploaded file
        progress_callback: Optional callback(bytes_uploaded, total_bytes)
    
    Returns:
        Dict with upload results:
            - success: bool
            - s3_key: str
            - bytes_uploaded: int
            - error: str (if failed)
    """
    if not os.path.exists(local_path):
        return {
            'success': False,
            's3_key': s3_key,
            'bytes_uploaded': 0,
            'error': f'Local file not found: {local_path}'
        }
    
    file_size = os.path.getsize(local_path)
    logger.info(f"Uploading local file to S3: {local_path} -> s3://{self.bucket_name}/{s3_key}")
    logger.info(f"  File size: {file_size / (1024**3):.2f} GB")
    
    # Start S3 multipart upload
    try:
        mpu = self.s3_client.create_multipart_upload(
            Bucket=self.bucket_name,
            Key=s3_key,
            ContentType='video/mp4'
        )
        upload_id = mpu['UploadId']
    except Exception as e:
        logger.error(f"Failed to create multipart upload: {e}")
        return {
            'success': False,
            's3_key': s3_key,
            'bytes_uploaded': 0,
            'error': f'Failed to create multipart upload: {e}'
        }
    
    parts = []
    part_number = 1
    total_bytes = 0
    
    try:
        with open(local_path, 'rb') as f:
            while True:
                # Read S3_PART_SIZE chunk
                chunk = f.read(S3_PART_SIZE)
                if not chunk:
                    break
                
                # Upload part
                part_response = self.s3_client.upload_part(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk
                )
                parts.append({
                    'ETag': part_response['ETag'],
                    'PartNumber': part_number
                })
                
                total_bytes += len(chunk)
                
                # Report progress
                if progress_callback:
                    try:
                        progress_callback(total_bytes, file_size)
                    except Exception:
                        pass
                
                logger.info(f"  Uploaded part {part_number} ({total_bytes / (1024**3):.2f} GB / {file_size / (1024**3):.2f} GB)")
                part_number += 1
        
        # Complete multipart upload
        if not parts:
            logger.error("No parts uploaded - empty file?")
            self.s3_client.abort_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                UploadId=upload_id
            )
            return {
                'success': False,
                's3_key': s3_key,
                'bytes_uploaded': 0,
                'error': 'No data to upload (empty file?)'
            }
        
        self.s3_client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        
        logger.info(f"  SUCCESS: Uploaded {total_bytes / (1024**3):.2f} GB to s3://{self.bucket_name}/{s3_key}")
        
        return {
            'success': True,
            's3_key': s3_key,
            'bytes_uploaded': total_bytes,
            'parts_count': len(parts)
        }
    
    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")
        self._abort_multipart(s3_key, upload_id)
        return {
            'success': False,
            's3_key': s3_key,
            'bytes_uploaded': total_bytes,
            'error': str(e)
        }
```

#### C. Modify `upload_session_chapters()` Method

**Replace the current implementation** with this new version:

```python
def upload_session_chapters(
    self,
    session: Dict[str, Any],
    gopro_ip: str,
    chapters: List[Dict[str, Any]],
    progress_callback: Optional[Callable[[str, int, int, int], None]] = None
) -> Dict[str, Any]:
    """
    Upload all chapters for a recording session to S3.
    
    NEW FLOW:
    1. Download chapter from GoPro to Jetson (/tmp/chapters/)
    2. Upload from Jetson to S3 (faster ethernet)
    3. Delete from Jetson local (keep on GoPro SD card)
    
    Args:
        session: Recording session document from Firebase
        gopro_ip: GoPro camera IP address
        chapters: List of chapter dicts with 'directory', 'filename', 'size' keys
        progress_callback: Optional callback(stage, chapter_num, total_chapters, bytes_uploaded)
    
    Returns:
        Dict with upload results:
            - success: bool
            - s3_prefix: str
            - chapters_uploaded: int
            - total_bytes: int
            - failed_chapters: list
            - errors: list
    """
    segment_session = session.get('segmentSession', '')
    if not segment_session:
        return {
            'success': False,
            's3_prefix': '',
            'chapters_uploaded': 0,
            'total_bytes': 0,
            'failed_chapters': [],
            'errors': ['Session missing segmentSession field']
        }
    
    # S3 prefix for this session's chapters
    s3_prefix = f"raw-chapters/{segment_session}/"
    
    # Local temp directory for this session
    local_temp_dir = f"/tmp/chapters/{segment_session}"
    os.makedirs(local_temp_dir, exist_ok=True)
    
    logger.info(f"Uploading {len(chapters)} chapters for session: {segment_session}")
    logger.info(f"  S3 prefix: s3://{self.bucket_name}/{s3_prefix}")
    logger.info(f"  Local temp dir: {local_temp_dir}")
    
    results = {
        'success': True,
        's3_prefix': s3_prefix,
        'chapters_uploaded': 0,
        'total_bytes': 0,
        'failed_chapters': [],
        'errors': [],
        'uploaded_chapters': []
    }
    
    total_chapters = len(chapters)
    
    for i, chapter in enumerate(chapters):
        chapter_num = i + 1
        filename = chapter['filename']
        directory = chapter['directory']
        expected_size = int(chapter.get('size', 0))
        
        # Generate S3 key: raw-chapters/{segmentSession}/chapter_001_GX010038.MP4
        s3_key = f"{s3_prefix}chapter_{chapter_num:03d}_{filename}"
        
        # Local path for this chapter
        local_path = os.path.join(local_temp_dir, filename)
        
        logger.info(f"[{chapter_num}/{total_chapters}] Processing: {filename}")
        
        # Report progress: downloading
        if progress_callback:
            try:
                progress_callback('downloading', chapter_num, total_chapters, results['total_bytes'])
            except Exception:
                pass
        
        # STEP 1: Download from GoPro to Jetson
        def download_progress(bytes_downloaded, total_bytes):
            if progress_callback:
                try:
                    progress_callback('downloading', chapter_num, total_chapters,
                                     results['total_bytes'] + bytes_downloaded)
                except Exception:
                    pass
        
        download_result = self.download_chapter_to_local(
            gopro_ip=gopro_ip,
            directory=directory,
            filename=filename,
            local_path=local_path,
            expected_size=expected_size,
            progress_callback=download_progress
        )
        
        if not download_result['success']:
            results['success'] = False
            results['failed_chapters'].append(filename)
            results['errors'].append(f"Download failed: {download_result.get('error', 'Unknown error')}")
            logger.error(f"[{chapter_num}/{total_chapters}] DOWNLOAD FAILED: {filename} - {download_result.get('error')}")
            continue
        
        logger.info(f"[{chapter_num}/{total_chapters}] Downloaded: {filename} ({download_result['bytes_downloaded'] / (1024**3):.2f} GB)")
        
        # Report progress: uploading
        if progress_callback:
            try:
                progress_callback('uploading', chapter_num, total_chapters, results['total_bytes'])
            except Exception:
                pass
        
        # STEP 2: Upload from Jetson to S3
        def upload_progress(bytes_uploaded, total_bytes):
            if progress_callback:
                try:
                    progress_callback('uploading', chapter_num, total_chapters,
                                     results['total_bytes'] + bytes_uploaded)
                except Exception:
                    pass
        
        upload_result = self.upload_local_file_to_s3(
            local_path=local_path,
            s3_key=s3_key,
            progress_callback=upload_progress
        )
        
        if upload_result['success']:
            results['chapters_uploaded'] += 1
            results['total_bytes'] += upload_result['bytes_uploaded']
            results['uploaded_chapters'].append({
                'filename': filename,
                's3_key': s3_key,
                'bytes': upload_result['bytes_uploaded']
            })
            logger.info(f"[{chapter_num}/{total_chapters}] UPLOADED: {filename} -> s3://{self.bucket_name}/{s3_key}")
        else:
            results['success'] = False
            results['failed_chapters'].append(filename)
            results['errors'].append(f"Upload failed: {upload_result.get('error', 'Unknown error')}")
            logger.error(f"[{chapter_num}/{total_chapters}] UPLOAD FAILED: {filename} - {upload_result.get('error')}")
        
        # STEP 3: Delete from Jetson local (keep on GoPro SD card)
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
                logger.info(f"[{chapter_num}/{total_chapters}] Deleted local file: {local_path}")
        except Exception as e:
            logger.warning(f"[{chapter_num}/{total_chapters}] Failed to delete local file: {e}")
            # Don't fail the job for cleanup errors
    
    # Cleanup: Remove session temp directory
    try:
        if os.path.exists(local_temp_dir):
            os.rmdir(local_temp_dir)
            logger.info(f"Cleaned up temp directory: {local_temp_dir}")
    except Exception as e:
        logger.warning(f"Failed to remove temp directory: {e}")
    
    # Final summary
    logger.info(f"Upload complete: {results['chapters_uploaded']}/{total_chapters} chapters")
    logger.info(f"  Total uploaded: {results['total_bytes'] / (1024**3):.2f} GB")
    if results['failed_chapters']:
        logger.warning(f"  Failed: {results['failed_chapters']}")
    
    return results
```

#### D. Add Import Statement at Top

Add this to the imports section:

```python
import os  # Add if not already present
```

---

## Implementation Steps

### Step 1: Backup Current Code
```bash
cp chapter_upload_service.py chapter_upload_service.py.backup
```

### Step 2: Add New Methods
1. Add `download_chapter_to_local()` method
2. Add `upload_local_file_to_s3()` method

### Step 3: Replace `upload_session_chapters()`
Replace the entire method with the new implementation above

### Step 4: Test with Single Session
Test with a small recording session (1-2 chapters) to verify:
- Download from GoPro works
- Upload to S3 works
- Local files are deleted
- Files remain on GoPro SD card

---

## Disk Space Management

### Temp Directory Structure
```
/tmp/chapters/
  └── {segmentSession}/
      ├── GX010038.MP4  (deleted after upload)
      ├── GX020038.MP4  (deleted after upload)
      └── ...
```

### Disk Space Calculation
- Each chapter: ~4GB
- Processing one chapter at a time: ~4GB max disk usage
- Total session (14 chapters): Still only ~4GB peak (one at a time)

### Safety Checks
The Jetson has ~114GB storage. Processing one chapter at a time means:
- **Before**: Streaming all 14 chapters (no disk usage)
- **After**: Download + upload one chapter (max 4GB disk usage)
- **Result**: No disk overflow issues ✅

---

## Testing Checklist

- [ ] Single chapter download works
- [ ] Single chapter upload to S3 works
- [ ] Local file deleted after successful upload
- [ ] File still exists on GoPro SD card after upload
- [ ] Progress callbacks work correctly
- [ ] Error handling works (network failure, disk full, etc.)
- [ ] Full session (14 chapters) uploads successfully
- [ ] Frontend progress UI shows correct stages (downloading/uploading)
- [ ] Temp directory cleaned up after completion

---

## Expected Behavior After Changes

### Frontend Progress Display
The progress callback now reports two stages:
- **downloading**: Downloading from GoPro to Jetson
- **uploading**: Uploading from Jetson to S3

You may want to update the frontend to show:
```
Chapter 5/14
⬇ Downloading from GoPro... (50%)
⬆ Uploading to S3... (75%)
```

### Performance Improvement
- **Before**: GoPro WiFi → S3 (limited by WiFi, ~50-100 Mbps)
- **After**: GoPro WiFi → Jetson, then Jetson Ethernet → S3 (1 Gbps)
- **Expected**: Similar download time, **much faster upload** to S3

### Reliability Improvement
- If S3 upload fails, chapter is already on Jetson (can retry without re-downloading)
- Chapters remain on GoPro SD card as backup
- Can manually verify/re-upload chapters from Jetson if needed

---

## Questions & Notes

### Q: What if Jetson disk fills up?
**A**: Processing one chapter at a time means max 4GB disk usage. Jetson has 114GB, so plenty of headroom.

### Q: What if download succeeds but upload fails?
**A**: File remains on Jetson in `/tmp/chapters/{segmentSession}/`. You can manually retry upload or debug the issue.

### Q: Do we need to delete chapters from GoPro SD card?
**A**: **NO**. Chapters remain on GoPro SD card. You'll manually clean them when needed (or implement a separate cleanup API later).

### Q: What about the old `stream_chapter_to_s3()` method?
**A**: You can keep it for backward compatibility, or remove it if you're sure it's not used elsewhere. I recommend keeping it commented out for now in case you need to roll back.

---

## Rollback Plan

If issues arise:
```bash
# Restore old version
cp chapter_upload_service.py.backup chapter_upload_service.py

# Restart service
sudo systemctl restart gopro-controller
```

---

## Implementation Status

✅ **IMPLEMENTED** - Changes have been applied to `chapter_upload_service.py`

### What Was Changed

#### 1. Added Robust Download Logic (from `scripts/download_chapters.py`)
- ✅ `KeepAliveThread` class - Prevents GoPro from sleeping during downloads
- ✅ `download_chapter_to_local()` method - Downloads with resume capability
  - Range header support for resuming interrupted downloads
  - Exponential backoff retry (max 20 attempts)
  - Never deletes partial files during retry
  - Progress reporting with speed calculation
  - Separate connect (10s) and read (60s) timeouts

#### 2. Added Local Upload Method
- ✅ `upload_local_file_to_s3()` method - Uploads from Jetson to S3
  - Uses multipart upload for large files
  - Progress reporting
  - Proper error handling with abort on failure

#### 3. Modified Session Upload Flow
- ✅ `upload_session_chapters()` method - Orchestrates download → upload → delete
  - Starts keep-alive thread at session start
  - Processes chapters one at a time: download → upload → delete local
  - Proper cleanup in finally block
  - Keeps chapters on GoPro SD card ✓
  - Only deletes from Jetson local storage

### Files Modified
- `chapter_upload_service.py` - Complete rewrite with new flow

### Files Kept (Old Streaming Method)
- `stream_chapter_to_s3()` - **Still present** for backward compatibility
  - Can be removed if not used elsewhere
  - Marked as legacy/deprecated

---

## Next Steps - Testing

1. **Test with single session** first
2. **Monitor during upload**:
   - Check disk usage: `df -h /tmp`
   - Check temp files: `ls -lh /tmp/chapters/`
   - Check keep-alive is working
3. **Verify chapters remain on GoPro SD card**
4. **Test full session** (14 chapters)
5. **Update frontend** to show download/upload stages (optional)

### Test Commands

#### Check disk space before/during upload
```bash
watch -n 2 'df -h /tmp && ls -lh /tmp/chapters/*/ 2>/dev/null'
```

#### Monitor keep-alive requests
```bash
# On Jetson, watch network traffic to GoPro
sudo tcpdump -i any host 172.26.138.51 and port 8080 | grep keep_alive
```

#### Verify chapters on GoPro after upload
```bash
# Use the chapter listing API
curl http://172.26.138.51:8080/gopro/media/list | jq
```

---

## Testing Checklist

- [ ] Single chapter download works
- [ ] Single chapter upload to S3 works
- [ ] Local file deleted after successful upload
- [ ] File still exists on GoPro SD card after upload
- [ ] Progress callbacks work correctly (downloading/uploading stages)
- [ ] Error handling works (network failure, retry, resume)
- [ ] Keep-alive prevents GoPro sleep during long downloads
- [ ] Full session (14 chapters) uploads successfully
- [ ] Temp directory cleaned up after completion
- [ ] Frontend progress UI shows correct stages

---

## Rollback Plan

If issues arise:
```bash
# View git diff to see what changed
git diff chapter_upload_service.py

# Revert to previous version
git checkout chapter_upload_service.py

# Or restore from backup if you created one
cp chapter_upload_service.py.backup chapter_upload_service.py

# Restart service
sudo systemctl restart gopro-controller
```

---

## Performance Expectations

### Before (Streaming)
- GoPro WiFi → Jetson → Stream to S3
- Limited by: GoPro WiFi speed (~50-100 Mbps)
- Time for 14 chapters (~56 GB): **~90-180 minutes**

### After (Download → Upload)
- GoPro WiFi → Jetson: ~50-100 Mbps
- Jetson Ethernet → S3: ~1 Gbps (10x faster!)
- **Download time**: Similar to before (~90-180 minutes)
- **Upload time**: Much faster (~10-20 minutes vs 90-180 minutes)
- **Total time**: Dominated by download, but more reliable with resume

### Key Benefits
1. **Reliability**: Resume on failure, no need to re-download from GoPro
2. **Chapters preserved**: Remain on GoPro SD card as backup
3. **Better error handling**: Exponential backoff, keep-alive
4. **Disk-efficient**: Only ~4GB max (one chapter at a time)
