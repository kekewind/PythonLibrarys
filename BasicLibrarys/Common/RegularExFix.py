import re


def file_path_fix(path):
    path = "".join(path.split())
    path = re.sub(r'[/*?"<>|\s]', r"", path)
    if path.count(':') > 1:
        path = "".join(path.split(':'))
        path = path[0:1] + ":" + path[1:]
    return path


def url_check(url):
    url_parser = re.compile(r"[a-zA-z]+://[^\s]*")
    if len(url_parser.findall(url)) > 0:
        return True
    else:
        return False


def download_url_check(url):
    if url.__contains__('.php?'):
        return False
    url_parser = re.compile(r"[a-zA-z]+://[^\s]*\....")
    if len(url_parser.findall(url)) > 0:
        return True
    else:
        return False


def url_fix(url):
    url = re.sub(r"[a-zA-z]+://[^\s]*\s\[img\]", r"", url)
    url = "".join(url.split()).rstrip().lstrip()
    return url


def json_fix(json):
    res = re.sub(r'(?<!\\)\\(?!["\\/bfnrt]|u[0-9a-fA-F]{4})', r"", json)
    return res
