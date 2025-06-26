def is_pdf(stream) -> bool:
    try:
        header = stream.read(8)
        return header.startswith(b"%PDF-")
    except:
        return False

def is_zip(stream) -> bool:
    try:
        header = stream.read(8)
        return header.startswith(b"PK\x03\x04")
    except:
        return False

def is_jpg(stream) -> bool:
    try:
        header = stream.read(2)
        return header.startswith(b'\xFF\xD8')
    except:
        return False


def is_png(stream) -> bool:
    try:
        header = stream.read(8)
        return header.startswith(b'\x89PNG\r\n\x1a\n')
    except:
        return False

def is_tar(stream) -> bool:
    try:
        stream.seek(257)
        magic = stream.read(8)
        return magic.startswith(b'ustar\x00\x30\x30') or magic.startswith(b'ustar\x20\x20\x00') 
    except:
        return False

def is_gz(stream) -> bool:
    try:
        header = stream.read(2)
        return header.startswith(b'\x1f\x8b')
    except:
        return False


typers = {
    'pdf': is_pdf,
    'zip': is_zip,
    'jpg': is_jpg,
    'png': is_png,
    'tar': is_tar,
    'gz': is_gz
}