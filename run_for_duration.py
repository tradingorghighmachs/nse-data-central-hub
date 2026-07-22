import subprocess
import time
import sys
import os

# Read duration from environment or default to 3.25 hours (11700 seconds)
RUN_DURATION = int(os.environ.get("RUN_DURATION_SECONDS", 11700))

def main():
    print(f"[*] Starting NSE Central Hub for a duration of {RUN_DURATION} seconds...")
    
    # Path to data_gateway.py (assuming it is in the same root directory in the NSE_Central_Hub repository)
    gateway_path = os.path.join(os.path.dirname(__file__), "data_gateway.py")
    if not os.path.exists(gateway_path):
        gateway_path = "data_gateway.py"

    if not os.path.exists(gateway_path):
        print(f"[-] Error: Could not find data_gateway.py")
        sys.exit(1)

    print(f"[+] Launching {gateway_path}...")
    
    # Start the data gateway in the background
    process = subprocess.Popen(
        [sys.executable, "-u", gateway_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    # Monitor output in real-time and print it immediately to prevent buffering
    start_time = time.time()
    try:
        while True:
            elapsed = time.time() - start_time
            if elapsed >= RUN_DURATION:
                print(f"\n[+] Target duration of {RUN_DURATION}s reached. Stopping process...")
                break
                
            # Check if process died early
            ret = process.poll()
            if ret is not None:
                print(f"[-] data_gateway.py exited unexpectedly with code {ret}")
                sys.exit(ret)

            # Read available output lines from the subprocess and print them unbuffered
            # Use non-blocking read or check stdout
            # To prevent blocking, we can read stdout in a loop with a small delay
            line = process.stdout.readline()
            if line:
                print(line.strip(), flush=True)
            else:
                time.sleep(0.1)
            
    except KeyboardInterrupt:
        print("[*] Interrupted by user. Shutting down...")

    # Terminate process gracefully
    try:
        print("[*] Sending SIGTERM to data_gateway...")
        process.terminate()
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        print("[!] Force killing process...")
        process.kill()
        process.wait()

    print("[SUCCESS] Swarm run completed. Ready to package logs.")

if __name__ == "__main__":
    main()
