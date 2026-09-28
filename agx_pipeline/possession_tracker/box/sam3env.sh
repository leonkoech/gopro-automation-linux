export LD_LIBRARY_PATH=$(ls -d /home/dev/.local/lib/python3.10/site-packages/nvidia/*/lib | tr "\n" ":")/usr/local/cuda-12.6/targets/aarch64-linux/lib:/usr/local/cuda-12.6/lib64:$LD_LIBRARY_PATH
export PYTHONPATH=/home/dev/sam3_libs:/home/dev/shot_typing
