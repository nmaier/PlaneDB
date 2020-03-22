import subprocess

SIZES = (16, 24, 32, 48, 64, 128)
SIZES = sorted(set([*SIZES, *[s * 2 for s in SIZES]]))
for s in SIZES:
    d = f"{s}.png"
    subprocess.check_call([
        r"C:\Program Files\Inkscape\inkscape.com",
        "icon.svg",
        "-e",
        d,
        "-C",
        "-w", str(s),
        "-h", str(s)
    ])
