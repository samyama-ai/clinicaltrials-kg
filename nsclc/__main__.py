"""Entry point for `python -m nsclc`.

Subcommands will be wired up in later build steps.
"""

import sys


def main() -> None:
    print("nsclc-radar: NSCLC Evidence Radar")
    print()
    print("Usage:  python -m nsclc <command>")
    print()
    print("Commands (not yet implemented):")
    print("  build-subset   Load NSCLC trial subset into the knowledge graph")
    print("  query          Run pre-built evidence queries")
    print("  report         Generate the evidence-radar report")
    sys.exit(0)


if __name__ == "__main__":
    main()
