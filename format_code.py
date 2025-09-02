#!/usr/bin/env python3
"""
Code formatting script to fix Black formatting issues.
This script installs dependencies and formats all Python files.
"""
import subprocess
import sys
import os
from pathlib import Path


def run_command(command, description):
    """Run a command and handle errors."""
    print(f"Running: {description}")
    try:
        result = subprocess.run(
            command, shell=True, check=True, capture_output=True, text=True
        )
        print(f"✓ {description} completed successfully")
        if result.stdout:
            print(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} failed")
        print(f"Error: {e.stderr}")
        return False


def main():
    """Main function to format code."""
    print("🔧 Starting code formatting process...")

    # Install dependencies
    if not run_command("pip install black flake8 isort", "Installing formatting tools"):
        print("Failed to install dependencies. Trying with requirements.txt...")
        if not run_command(
            "pip install -r requirements.txt", "Installing from requirements.txt"
        ):
            print("❌ Failed to install dependencies")
            return False

    # Format imports with isort
    if not run_command("python -m isort . --profile black", "Sorting imports"):
        print("⚠️ Import sorting failed, continuing...")

    # Format code with Black
    if not run_command("python -m black .", "Formatting code with Black"):
        print("❌ Code formatting failed")
        return False

    # Run flake8 to check for remaining issues
    print("\n📋 Running code quality checks...")
    run_command(
        "python -m flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics",
        "Checking for syntax errors",
    )

    print("\n✅ Code formatting completed!")
    print("📝 Next steps:")
    print("1. Review the changes made by Black")
    print("2. Commit the formatted code")
    print("3. Push to trigger CI pipeline")

    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
