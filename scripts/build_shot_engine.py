"""Build the shot detector's TensorRT FP16 engine on the AGX (SHOT_DET_ENGINE in .env.agx points at it).

    cd /home/dev/gopro-automation-linux && set -a && . ./.env.agx && set +a
    python3 scripts/build_shot_engine.py [weights.pt] [out.engine]

Profile: batch 1..16, image sides 320..640, tuned for 16 x 640x480. The detector letterboxes its
720x540 frames to 640x480 (rect inference), so the engine MUST accept that shape: a 640x640-only
engine returns garbage for it (14 Oct 2026: 39 of 3512 ball frames found). Ultralytics' own dynamic
export spans 32..1280 px, which TensorRT 10.3 on the Orin took over 50 min to tune without finishing.

An engine only runs on the device + TensorRT version that built it: rebuild after a JetPack/TensorRT
upgrade. The detector checks it at load and falls back to the .pt if it does not load or answer.
The output carries Ultralytics' header (int32 length + JSON metadata) so YOLO(<engine>) loads it.
Takes ~20-25 min on the Orin; run it with no game recording (it uses the whole GPU).
"""
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
WEIGHTS = os.path.join(HERE, "..", "agx_pipeline", "shot_detect", "weights")
PT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(WEIGHTS, "ball_yolo26s_gray_hifps_v3_best.pt")
OUT = sys.argv[2] if len(sys.argv) > 2 else os.path.splitext(PT)[0] + ".fp16_b16_320-640.engine"
BATCH, SIDE, OPT_H, MIN_SIDE = 16, 640, 480, 320


def main() -> None:
    import onnx
    import tensorrt as trt
    from ultralytics import YOLO

    onnx_path = YOLO(PT).export(format="onnx", dynamic=True, imgsz=SIDE, batch=BATCH)
    logger = trt.Logger(trt.Logger.WARNING)
    builder = trt.Builder(logger)
    network = builder.create_network(0)
    parser = trt.OnnxParser(network, logger)
    with open(onnx_path, "rb") as fh:
        if not parser.parse(fh.read()):
            raise SystemExit("ONNX parse failed: " + "; ".join(
                str(parser.get_error(i)) for i in range(parser.num_errors)))
    config = builder.create_builder_config()
    config.set_memory_pool_limit(trt.MemoryPoolType.WORKSPACE, 2 << 30)
    config.set_flag(trt.BuilderFlag.FP16)
    profile = builder.create_optimization_profile()
    profile.set_shape(network.get_input(0).name, (1, 3, MIN_SIDE, MIN_SIDE),
                      (BATCH, 3, OPT_H, SIDE), (BATCH, 3, SIDE, SIDE))
    config.add_optimization_profile(profile)

    t = time.time()
    engine = builder.build_serialized_network(network, config)
    if engine is None:
        raise SystemExit("TensorRT engine build failed")
    meta = {p.key: p.value for p in onnx.load(onnx_path).metadata_props}
    meta.update(batch=str(BATCH), imgsz=str([SIDE, SIDE]))
    blob = json.dumps(meta)
    tmp = OUT + ".tmp"
    with open(tmp, "wb") as fh:
        fh.write(len(blob).to_bytes(4, byteorder="little", signed=True))
        fh.write(blob.encode())
        fh.write(engine)
    os.replace(tmp, OUT)
    os.remove(onnx_path)
    print("built %s in %.0f s (%.1f MB)" % (OUT, time.time() - t, engine.nbytes / 1e6))


if __name__ == "__main__":
    main()
