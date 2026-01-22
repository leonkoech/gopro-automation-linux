#!/usr/bin/env python3
"""
Jetson Manager - Local tool for managing Jetson devices

Features:
- Discover Jetsons via Tailscale API
- SSH access without manual connection
- View service logs in real-time
- Restart services
- Test endpoints
- Deploy code changes
- Run commands across all Jetsons

Usage:
    python jetson_manager.py [command] [options]

Commands:
    status      - Show status of all Jetsons
    logs        - View service logs (backend/frontend)
    ssh         - Open SSH session to a Jetson
    restart     - Restart a service on Jetsons
    deploy      - Deploy code to Jetsons
    exec        - Execute command on Jetsons
    health      - Check health of all services
"""

import os
import sys
import json
import argparse
import subprocess
import requests
from pathlib import Path
from typing import Optional, List, Dict
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
SCRIPT_DIR = Path(__file__).parent.resolve()
SSH_KEY_PATH = SCRIPT_DIR / "id_rsa"
TAILSCALE_API_KEY = os.getenv("TAILSCALE_API_KEY", "tskey-api-k6ussKkux221CNTRL-z6meRYNS3xSpnQqsRbxKxSzQfoigxkFDd")
TAILSCALE_API_BASE = "https://api.tailscale.com/api/v2"

# Default SSH user
SSH_USER = "developer"

# Service configurations (matching existing Jetson setup)
# Only backend runs on Jetsons - frontend is deployed to Firebase Hosting
SERVICES = {
    "backend": {
        "name": "gopro-controller",
        "port": 5000,
        "health_endpoint": "/health",
        "log_file": None,
        "journal_unit": "gopro-controller",
        "deploy_path": "/home/developer/Development/gopro-automation-linux",
    },
}


@dataclass
class Jetson:
    """Represents a Jetson device"""
    name: str
    hostname: str
    ip: str
    online: bool
    last_seen: str
    os: str

    @property
    def display_name(self) -> str:
        return self.hostname or self.name


class Colors:
    """ANSI color codes for terminal output"""
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[0;33m'
    BLUE = '\033[0;34m'
    CYAN = '\033[0;36m'
    BOLD = '\033[1m'
    NC = '\033[0m'  # No Color

    @classmethod
    def success(cls, text: str) -> str:
        return f"{cls.GREEN}{text}{cls.NC}"

    @classmethod
    def error(cls, text: str) -> str:
        return f"{cls.RED}{text}{cls.NC}"

    @classmethod
    def warning(cls, text: str) -> str:
        return f"{cls.YELLOW}{text}{cls.NC}"

    @classmethod
    def info(cls, text: str) -> str:
        return f"{cls.CYAN}{text}{cls.NC}"

    @classmethod
    def bold(cls, text: str) -> str:
        return f"{cls.BOLD}{text}{cls.NC}"


def print_header(title: str):
    """Print a formatted header"""
    width = 60
    print("\n" + "=" * width)
    print(f"  {Colors.bold(title)}")
    print("=" * width)


def print_status(name: str, status: str, success: bool):
    """Print a status line"""
    symbol = Colors.success("[OK]") if success else Colors.error("[FAIL]")
    print(f"  {symbol} {name}: {status}")


def discover_jetsons() -> List[Jetson]:
    """Discover Jetson devices via Tailscale API"""
    headers = {"Authorization": f"Bearer {TAILSCALE_API_KEY}"}

    try:
        response = requests.get(
            f"{TAILSCALE_API_BASE}/tailnet/-/devices",
            headers=headers,
            timeout=10
        )
        response.raise_for_status()

        devices = response.json().get("devices", [])
        jetsons = []

        for device in devices:
            hostname = device.get("hostname", "").lower()
            if "jetson" in hostname:
                addresses = device.get("addresses", [])
                ipv4 = next((addr for addr in addresses if "." in addr), None)

                jetsons.append(Jetson(
                    name=device.get("name", ""),
                    hostname=device.get("hostname", ""),
                    ip=ipv4,
                    online=device.get("connectedToControl", False),
                    last_seen=device.get("lastSeen", ""),
                    os=device.get("os", ""),
                ))

        return jetsons

    except requests.RequestException as e:
        print(Colors.error(f"Failed to discover Jetsons: {e}"))
        return []


def run_ssh_command(jetson: Jetson, command: str, timeout: int = 30) -> tuple[bool, str]:
    """Run a command on a Jetson via SSH"""
    if not SSH_KEY_PATH.exists():
        return False, f"SSH key not found: {SSH_KEY_PATH}"

    ssh_cmd = [
        "ssh",
        "-i", str(SSH_KEY_PATH),
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", f"ConnectTimeout={timeout}",
        "-o", "LogLevel=ERROR",
        f"{SSH_USER}@{jetson.ip}",
        command
    ]

    try:
        result = subprocess.run(
            ssh_cmd,
            capture_output=True,
            text=True,
            timeout=timeout + 5
        )
        return result.returncode == 0, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return False, "Command timed out"
    except Exception as e:
        return False, str(e)


def open_ssh_session(jetson: Jetson):
    """Open an interactive SSH session to a Jetson"""
    if not SSH_KEY_PATH.exists():
        print(Colors.error(f"SSH key not found: {SSH_KEY_PATH}"))
        return

    print(Colors.info(f"Connecting to {jetson.display_name} ({jetson.ip})..."))

    ssh_cmd = [
        "ssh",
        "-i", str(SSH_KEY_PATH),
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        f"{SSH_USER}@{jetson.ip}"
    ]

    try:
        subprocess.run(ssh_cmd)
    except KeyboardInterrupt:
        print("\nSSH session ended.")


def stream_logs(jetson: Jetson, service: str, lines: int = 50, follow: bool = False):
    """Stream logs from a Jetson service"""
    service_config = SERVICES.get(service)
    if not service_config:
        print(Colors.error(f"Unknown service: {service}"))
        return

    print_header(f"Logs: {service} on {jetson.display_name}")

    # Try journalctl first, then log file
    if follow:
        cmd = f"sudo journalctl -u {service_config['journal_unit']} -f --no-pager -n {lines}"
    else:
        cmd = f"sudo journalctl -u {service_config['journal_unit']} --no-pager -n {lines}"

    print(Colors.info(f"Streaming logs (Ctrl+C to stop)...\n"))

    ssh_cmd = [
        "ssh",
        "-i", str(SSH_KEY_PATH),
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=ERROR",
        f"{SSH_USER}@{jetson.ip}",
        cmd
    ]

    try:
        process = subprocess.Popen(
            ssh_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        for line in process.stdout:
            print(line, end="")

        process.wait()
    except KeyboardInterrupt:
        print("\n" + Colors.info("Log streaming stopped."))


def check_health(jetson: Jetson, service: str) -> tuple[bool, str]:
    """Check health of a service on a Jetson"""
    service_config = SERVICES.get(service)
    if not service_config:
        return False, f"Unknown service: {service}"

    port = service_config["port"]
    endpoint = service_config["health_endpoint"]

    cmd = f"curl -s -o /dev/null -w '%{{http_code}}' http://localhost:{port}{endpoint} 2>/dev/null || echo 'FAIL'"
    success, output = run_ssh_command(jetson, cmd, timeout=10)

    if success and output.strip() in ["200", "304"]:
        return True, f"HTTP {output.strip()}"
    elif "FAIL" in output or not success:
        return False, "Service not responding"
    else:
        return False, f"HTTP {output.strip()}"


def restart_service(jetson: Jetson, service: str) -> tuple[bool, str]:
    """Restart a service on a Jetson"""
    service_config = SERVICES.get(service)
    if not service_config:
        return False, f"Unknown service: {service}"

    cmd = f"sudo systemctl restart {service_config['journal_unit']}"
    success, output = run_ssh_command(jetson, cmd, timeout=30)

    if success:
        return True, "Service restarted"
    else:
        return False, output


def get_service_status(jetson: Jetson, service: str) -> tuple[bool, str]:
    """Get status of a service on a Jetson"""
    service_config = SERVICES.get(service)
    if not service_config:
        return False, f"Unknown service: {service}"

    cmd = f"systemctl is-active {service_config['journal_unit']} 2>/dev/null || echo 'inactive'"
    success, output = run_ssh_command(jetson, cmd, timeout=10)

    status = output.strip()
    is_active = status == "active"
    return is_active, status


def select_jetson(jetsons: List[Jetson], selection: Optional[str] = None) -> Optional[Jetson]:
    """Interactive Jetson selection"""
    online_jetsons = [j for j in jetsons if j.online]

    if not online_jetsons:
        print(Colors.error("No online Jetsons found!"))
        return None

    if selection:
        # Find by name or number
        for j in online_jetsons:
            if selection.lower() in j.hostname.lower() or selection == j.ip:
                return j

        # Try as index
        try:
            idx = int(selection) - 1
            if 0 <= idx < len(online_jetsons):
                return online_jetsons[idx]
        except ValueError:
            pass

        print(Colors.error(f"Jetson not found: {selection}"))
        return None

    if len(online_jetsons) == 1:
        return online_jetsons[0]

    # Interactive selection
    print("\nSelect a Jetson:")
    for i, j in enumerate(online_jetsons, 1):
        print(f"  {i}. {j.display_name} ({j.ip})")

    try:
        choice = input("\nEnter number or name: ").strip()
        return select_jetson(jetsons, choice)
    except (KeyboardInterrupt, EOFError):
        return None


# ============== Command Handlers ==============

def cmd_status(args):
    """Show status of all Jetsons and services"""
    print_header("Jetson Fleet Status")

    jetsons = discover_jetsons()
    if not jetsons:
        print(Colors.error("No Jetsons found!"))
        return

    for jetson in jetsons:
        status_icon = Colors.success("ONLINE") if jetson.online else Colors.error("OFFLINE")
        print(f"\n{Colors.bold(jetson.display_name)}")
        print(f"  IP: {jetson.ip}")
        print(f"  Status: {status_icon}")
        print(f"  OS: {jetson.os}")

        if jetson.online:
            # Check services
            for service_name in SERVICES.keys():
                is_active, status = get_service_status(jetson, service_name)
                healthy, health_status = check_health(jetson, service_name)

                if is_active and healthy:
                    print(f"  {service_name}: {Colors.success('Running')} ({health_status})")
                elif is_active:
                    print(f"  {service_name}: {Colors.warning('Running but unhealthy')} ({health_status})")
                else:
                    print(f"  {service_name}: {Colors.error(status)}")


def cmd_logs(args):
    """View logs from a Jetson service"""
    jetsons = discover_jetsons()
    jetson = select_jetson(jetsons, args.jetson)

    if not jetson:
        return

    stream_logs(jetson, args.service, lines=args.lines, follow=args.follow)


def cmd_ssh(args):
    """Open SSH session to a Jetson"""
    jetsons = discover_jetsons()
    jetson = select_jetson(jetsons, args.jetson)

    if not jetson:
        return

    open_ssh_session(jetson)


def cmd_restart(args):
    """Restart a service on Jetsons"""
    jetsons = discover_jetsons()

    if args.all:
        targets = [j for j in jetsons if j.online]
    else:
        jetson = select_jetson(jetsons, args.jetson)
        if not jetson:
            return
        targets = [jetson]

    print_header(f"Restarting {args.service}")

    for jetson in targets:
        print(f"\n{jetson.display_name}:")
        success, msg = restart_service(jetson, args.service)
        print_status("Restart", msg, success)

        if success:
            # Wait and check health
            import time
            time.sleep(3)
            healthy, status = check_health(jetson, args.service)
            print_status("Health", status, healthy)


def cmd_exec(args):
    """Execute command on Jetsons"""
    jetsons = discover_jetsons()

    if args.all:
        targets = [j for j in jetsons if j.online]
    else:
        jetson = select_jetson(jetsons, args.jetson)
        if not jetson:
            return
        targets = [jetson]

    command = " ".join(args.cmd)
    print_header(f"Executing: {command}")

    for jetson in targets:
        print(f"\n{Colors.bold(jetson.display_name)} ({jetson.ip}):")
        print("-" * 40)
        success, output = run_ssh_command(jetson, command, timeout=args.timeout)
        if output:
            print(output)
        if not success:
            print(Colors.error("Command failed"))


def cmd_health(args):
    """Check health of all services on all Jetsons"""
    print_header("Health Check")

    jetsons = discover_jetsons()
    online_jetsons = [j for j in jetsons if j.online]

    if not online_jetsons:
        print(Colors.error("No online Jetsons found!"))
        return

    results = []

    for jetson in online_jetsons:
        print(f"\n{Colors.bold(jetson.display_name)}:")

        for service_name in SERVICES.keys():
            healthy, status = check_health(jetson, service_name)
            print_status(service_name, status, healthy)
            results.append((jetson.display_name, service_name, healthy))

    # Summary
    print("\n" + "-" * 40)
    total = len(results)
    passed = sum(1 for _, _, h in results if h)
    print(f"Total: {passed}/{total} services healthy")


def cmd_deploy(args):
    """Deploy code to Jetsons (for local testing)"""
    jetsons = discover_jetsons()

    if args.all:
        targets = [j for j in jetsons if j.online]
    else:
        jetson = select_jetson(jetsons, args.jetson)
        if not jetson:
            return
        targets = [jetson]

    service = args.service
    print_header(f"Deploying {service}")

    for jetson in targets:
        print(f"\n{Colors.bold(jetson.display_name)}:")

        if service == "backend":
            deploy_cmd = """
            cd ~/gopro-automation-linux &&
            git pull origin main &&
            source venv/bin/activate &&
            pip install -r requirements.txt -q &&
            sudo systemctl restart gopro-backend &&
            echo 'Deploy complete!'
            """
        else:
            deploy_cmd = """
            cd ~/gopro-automation-wb &&
            git pull origin main &&
            npm install --production &&
            sudo systemctl restart gopro-frontend &&
            echo 'Deploy complete!'
            """

        success, output = run_ssh_command(jetson, deploy_cmd, timeout=120)
        print(output)
        print_status("Deploy", "Complete" if success else "Failed", success)


def main():
    parser = argparse.ArgumentParser(
        description="Jetson Manager - Manage GoPro Automation Jetson devices",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s status                    # Show all Jetsons status
  %(prog)s logs backend              # View backend logs
  %(prog)s logs backend -f           # Follow backend logs
  %(prog)s ssh                       # SSH to a Jetson (interactive)
  %(prog)s ssh jetson-nano-001       # SSH to specific Jetson
  %(prog)s restart backend --all     # Restart backend on all Jetsons
  %(prog)s exec -a "df -h"            # Run command on all Jetsons
  %(prog)s health                    # Check health of all services
  %(prog)s deploy backend --all      # Deploy backend to all Jetsons
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # status
    subparsers.add_parser("status", help="Show status of all Jetsons")

    # logs
    logs_parser = subparsers.add_parser("logs", help="View service logs")
    logs_parser.add_argument("service", choices=["backend"], help="Service to view logs for")
    logs_parser.add_argument("-j", "--jetson", help="Jetson name or IP")
    logs_parser.add_argument("-n", "--lines", type=int, default=50, help="Number of lines to show")
    logs_parser.add_argument("-f", "--follow", action="store_true", help="Follow log output")

    # ssh
    ssh_parser = subparsers.add_parser("ssh", help="SSH to a Jetson")
    ssh_parser.add_argument("jetson", nargs="?", help="Jetson name or IP")

    # restart
    restart_parser = subparsers.add_parser("restart", help="Restart a service")
    restart_parser.add_argument("service", choices=["backend"], help="Service to restart")
    restart_parser.add_argument("-j", "--jetson", help="Jetson name or IP")
    restart_parser.add_argument("-a", "--all", action="store_true", help="Restart on all Jetsons")

    # exec
    exec_parser = subparsers.add_parser("exec", help="Execute command on Jetsons")
    exec_parser.add_argument("cmd", nargs="+", help="Command to execute")
    exec_parser.add_argument("-j", "--jetson", help="Jetson name or IP")
    exec_parser.add_argument("-a", "--all", action="store_true", help="Execute on all Jetsons")
    exec_parser.add_argument("-t", "--timeout", type=int, default=30, help="Command timeout")

    # health
    subparsers.add_parser("health", help="Check health of all services")

    # deploy
    deploy_parser = subparsers.add_parser("deploy", help="Deploy code to Jetsons")
    deploy_parser.add_argument("service", choices=["backend"], help="Service to deploy")
    deploy_parser.add_argument("-j", "--jetson", help="Jetson name or IP")
    deploy_parser.add_argument("-a", "--all", action="store_true", help="Deploy to all Jetsons")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Verify SSH key exists
    if not SSH_KEY_PATH.exists():
        print(Colors.error(f"SSH key not found: {SSH_KEY_PATH}"))
        print("Please ensure id_rsa is in the project directory")
        sys.exit(1)

    # Ensure key has correct permissions
    os.chmod(SSH_KEY_PATH, 0o600)

    # Route to command handler
    handlers = {
        "status": cmd_status,
        "logs": cmd_logs,
        "ssh": cmd_ssh,
        "restart": cmd_restart,
        "exec": cmd_exec,
        "health": cmd_health,
        "deploy": cmd_deploy,
    }

    handler = handlers.get(args.command)
    if handler:
        try:
            handler(args)
        except KeyboardInterrupt:
            print("\n" + Colors.info("Operation cancelled."))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
