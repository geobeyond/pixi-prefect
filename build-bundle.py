"""Build bundles for moving into airgapped nodes.

This script should be run on a network-accessible ubuntu:24.04 machine,
as it is a similar environment to the one expected in the airgapped nodes.
It uses the following external tools, which should be available on a default
ubuntu installation:

- apt
- gpg
- python
- wget

It also uses the `pixi pack` command, which must have been previously
installed by leveraging the provided `install-pixi.sh` file.
"""

import os
import re
import shlex
import shutil
import tarfile
import tempfile
from pathlib import Path
from subprocess import (
    CalledProcessError,
    run,
)
from typing import Iterable


def get_system_update_packages() -> list[str]:
    """Get all packages that would be upgraded or installed during apt upgrade."""
    result = run(
        shlex.split("apt upgrade --simulate"),
        capture_output=True,
        text=True,
        check=True
    )
    packages = set()
    for line in result.stdout.split("\n"):
        if line.startswith("Inst "):
            parts = line.split()
            if len(parts) >= 2:
                package_name = parts[1]
                packages.add(package_name)
    return sorted(packages)


def build_pag_controller_bundle(version: str | None = None) -> None:
    base_dir = Path(__file__).parent
    pixi_pack_dir = base_dir / "vendored"
    target_dir = base_dir / "pag-software-bundle"
    target_archive = base_dir / f"pag-software-bundle{f'-{version}' if version else ''}.tar.gz"
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        print("Updating package lists...")
        run(
            ["sudo", "apt", "update"],
            capture_output=True,
            text=True,
            check=True
        )
        print("Collecting system update packages...")
        system_updates = get_system_update_packages()
        if system_updates:
            download_dir = target_dir / "offline-packages/system-updates"
            print(f"Downloading {len(system_updates)} system update packages to '{download_dir}'...")
            download_deb_packages(download_dir, system_updates)
        else:
            print("No system updates available.")

        print("Copying pixi binary package...")
        shutil.copy(base_dir / "vendored/pixi", target_dir)
        print("Copying pixi-unpack...")
        shutil.copy(base_dir / "vendored/pixi-unpack", target_dir)

        for controller_dependency_name in ("ansible", "rsync"):
            sub_dependencies = get_deb_package_dependencies(controller_dependency_name)
            download_dir = target_dir / f"offline-packages/{controller_dependency_name}"
            print(f"Downloading deb packages to '{download_dir}'...")
            download_deb_packages(download_dir, sub_dependencies)

        print("Including ansible deployment files...")
        shutil.copytree(
            base_dir / "ansible",
            target_dir / "ansible",
            ignore=shutil.ignore_patterns("inventory.yml", "secrets.yml")
        )

        print("Including Prefect flows...")
        shutil.copytree(
            base_dir / "flows",
            target_dir / "flows",
            ignore=shutil.ignore_patterns(".pixi", "__pycache__")
        )

        print("Adding postgresql debian repository...")
        prepare_postgresql_repository()

        print("Updating system package list...")
        run(
            shlex.split("sudo apt update"),
            capture_output=True,
            text=True,
            check=True
        )

        deb_packages = set()
        for server_dependency_name in (
                "acl",
                "postgresql-17",
        ):
            sub_dependencies = get_deb_package_dependencies(server_dependency_name)
            deb_packages.update(sub_dependencies)
            download_dir = target_dir / f"offline-packages/{server_dependency_name}"
            print(f"Downloading deb packages to '{download_dir}'...")
            download_deb_packages(download_dir, sub_dependencies)

        print("Packing pixi environment...")
        run(
            shlex.split(f"chmod +x pixi-pack"),
            capture_output=True,
            cwd=pixi_pack_dir,
            text=True,
            check=True
        )
        run(
            shlex.split(f"./pixi-pack {base_dir / 'pixi.toml'} --output-file {target_dir / 'prefect-base.tar'}"),
            capture_output=True,
            cwd=pixi_pack_dir,
            text=True,
            check=True
        )

        print(f"Writing script...")
        write_install_script(target_dir)

        print(f"Generating archive '{target_archive}'...")
        generate_archive(target_dir, target_archive)

        print("Cleaning up...")
        remove_dir(target_dir)
        _remove_deb_repository_from_apt_sources(
            "postgresql.list", "postgresql.gpg")

    except CalledProcessError as err:
        print(f"Command failed: {err.cmd}")
        print(err.stdout)
        print(err.stderr)
        raise SystemExit(err.returncode)

    print("Done!")


def write_install_script(target_dir: Path) -> None:
    contents = (
        "#!/bin/bash\n"
        "set -o errexit\n\n"
        "# 1. Apply system updates (if available)\n"
        "if [ -d offline-packages/system-updates ]; then\n"
        "    echo 'Applying system updates...'\n"
        "    pushd offline-packages/system-updates\n"
        "    sudo dpkg --install ./*.deb || sudo apt install --yes --fix-broken --allow-downgrades ./*.deb\n"
        "    popd\n"
        "fi\n"
        "\n"
        "# 2. Install pag-controller system dependencies, which are located in\n"
        "# the `offline-packages` directory\n"
        "echo 'Installing rsync...'\n"
        "pushd offline-packages/rsync\n"
        "sudo dpkg --install ./*.deb || sudo apt install --yes --fix-broken --allow-downgrades ./*.deb\n"
        "popd\n"
        "\n"
        "echo 'Installing ansible...'\n"
        "pushd offline-packages/ansible\n"
        "sudo dpkg --install ./*.deb || sudo apt install --yes --fix-broken --allow-downgrades ./*.deb\n"
        "popd\n"
        "\n"
        "echo 'Done - Now modify the provided ansible inventory and secrets files'\n"
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "install-pag-controller.sh"
    target_file.write_text(contents)
    target_file.chmod(0o755)


def remove_dir(target_dir: Path) -> None:
    run(
        shlex.split(f"rm --recursive --force {target_dir}"),
        capture_output=True,
        text=True,
        check=True
    )


def generate_archive(source_dir: Path, target_path: Path) -> None:
    with tarfile.open(str(target_path), "w:gz") as tar:
        tar.add(source_dir, arcname=source_dir.name)


def download_deb_packages(target_dir: Path , packages: Iterable[str]) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    run(
        shlex.split(f"apt download {' '.join(packages)}"),
        cwd=target_dir,
        capture_output=True,
        text=True,
        check=True
    )


def get_deb_package_dependencies(package: str) -> list[str]:
    raw_result = run(
        shlex.split(
            f"apt-cache depends --recurse --no-recommends --no-suggests "
            f"--no-conflicts --no-breaks --no-replaces --no-enhances "
            f"{package}"
        ),
        capture_output=True,
        text=True,
        check=True
    )
    return sorted(
        [
            line for line in raw_result.stdout.split("\n")
            if re.search(r"^\w", line) is not None and ":" not in line
        ]
    )


def prepare_postgresql_repository():
    with tempfile.TemporaryDirectory() as temp_dir:
        work_dir = Path(temp_dir)
        gpg_keys_file = work_dir / "postgresql.gpg"
        apt_list_file = work_dir / "postgresql.list"
        _get_gpg_keys(
            gpg_keys_file,
            "https://www.postgresql.org/media/keys/ACCC4CF8.asc"
        )
        _add_deb_repository_to_apt_sources(
            sources_line="deb http://apt.postgresql.org/pub/repos/apt noble-pgdg main",
            target_file=apt_list_file,
            gpg_keys_file=gpg_keys_file
        )


# TODO: Remove this
def prepare_caddy_repository():
    with tempfile.TemporaryDirectory() as temp_dir:
        work_dir = Path(temp_dir)
        gpg_keys_file = work_dir / "caddy.gpg"
        apt_list_file = work_dir / "caddy.list"
        _get_gpg_keys(
            gpg_keys_file,
            "https://dl.cloudsmith.io/public/caddy/stable/gpg.key"
        )
        _add_deb_repository_to_apt_sources(
            "deb https://dl.cloudsmith.io/public/caddy/stable/deb/debian any-version main",
            apt_list_file,
            gpg_keys_file
        )


def _get_gpg_keys(target_path: Path, keys_url: str) -> None:
    download_result = run(
        shlex.split(f"wget --quiet --output-document - {keys_url}"),
        capture_output=True,
        check=True
    )
    dearmor_keys_result = run(
        shlex.split(f"gpg --dearmor"),
        input=download_result.stdout,
        capture_output=True,
        check=True
    )
    target_path.write_bytes(dearmor_keys_result.stdout)


def _add_deb_repository_to_apt_sources(
        sources_line,
        target_file: Path,
        gpg_keys_file: Path,
):
    target_file.write_text(sources_line)
    run(
        shlex.split("sudo mkdir --parents /etc/apt/sources.list.d"),
        capture_output=True,
        text=True,
        check=True
    )
    run(
        shlex.split("sudo mkdir --parents /etc/apt/trusted.gpg.d"),
        capture_output=True,
        text=True,
        check=True
    )
    run(
        shlex.split(f"sudo cp {gpg_keys_file} /etc/apt/trusted.gpg.d"),
        capture_output=True,
        text=True,
        check=True
    )
    run(
        shlex.split(f"sudo cp {target_file} /etc/apt/sources.list.d"),
        capture_output=True,
        text=True,
        check=True
    )
    return target_file


def _remove_deb_repository_from_apt_sources(
        apt_list_file_name: str, gpg_keys_file_name: str
) -> None:
    target_apt_list_path = Path("/etc/apt/sources.list.d") / apt_list_file_name
    if target_apt_list_path.exists():
        run(
            shlex.split(f"sudo rm {target_apt_list_path}"),
            capture_output=True,
            text=True,
            check=True
        )
    target_gpg_keys_path = Path("/etc/apt/trusted.gpg.d") / gpg_keys_file_name
    if target_gpg_keys_path.exists():
        run(
            shlex.split(f"sudo rm {target_gpg_keys_path}"),
            capture_output=True,
            text=True,
            check=True
        )


if __name__ == "__main__":
    version = os.getenv("BUNDLE_VERSION")
    build_pag_controller_bundle(version)
