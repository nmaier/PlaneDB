from io import BytesIO
from struct import unpack, pack
from path import Path

def getimages(f):
    with f.open("rb") as fp:
        headers = list()
        (_, _, items) = unpack("HHH", fp.read(6))
        for _ in range(items):
            (width, _, _, _, planes, bits, size, offset) = unpack("BBBBHHII", fp.read(16))
            if width == 0:
                width = 256
            headers += [width, planes, bits, size, offset],
        for header in headers:
            width = header[0]
            d = Path(f"{width}.png")
            if d.isfile() and width >= 96:
                with d.open("rb") as picp:
                    header[2] = 1
                    header[3] = 32
                    header += picp.read(),
            else:
                (size, offset) = header[-2:]
                fp.seek(offset, 0)
                header += fp.read(size),
        return headers

def main():
    imgs = getimages(Path("icon.ico"))
    with BytesIO() as out:
        out.write(b"\x00\x00\x01\x00")
        out.write(pack("H", len(imgs)))
        cur = 0
        for i in imgs:
            (width, planes, bits, *_, data) = i
            length = len(data)
            dim = width if width < 256 else 0
            out.write(pack(
                "BBBBHHLL",
                dim, dim, 0, 0,
                planes, bits,
                length, cur + 6 + 0x10 * len(imgs)
            ))
            cur += length
        for (*_, d) in imgs:
            out.write(d)
        with open("icon-compressed.ico", "wb") as outp:
            outp.write(out.getvalue())

if __name__ == "__main__":
    main()
