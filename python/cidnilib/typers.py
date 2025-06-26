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


typers = {
    'pdf': is_pdf,
    'zip': is_zip
}