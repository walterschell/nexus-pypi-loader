import argparse
import os
import hashlib
import re
import json
from typing import Optional, Callable, Iterator, cast
import urllib.parse
import time

class ProgressLogger:
    def __init__(self, max_items: int) -> None:
        self.max_items = max_items
        self.cur_items = 0
        self.next_update = None

    def report_item(self, item):
        self.cur_items += 1
        if self.next_update is not None and time.time() < self.next_update:
             return
        pct = 100 * self.cur_items / self.max_items
        print(f"{item} - {self.cur_items} / {self.max_items} ({pct:.1f}% complete)")
        self.next_update = time.time() + 2.0


class PyPiPackageName:
    def __init__(self, name: str) -> None:
        self.name = name
        self._comparable_name = name.replace("_", "-").replace(".","-").lower()

    def __hash__(self) -> int:
        return hash(self._comparable_name)

    def __eq__(self, __o: object) -> bool:
        assert type(__o) == PyPiPackageName
        __o = cast(PyPiPackageName, __o)
        return self._comparable_name == __o._comparable_name

    def __str__(self):
        return self.name

    def __repr__(self) -> str:
        return f"{self.name} ({self._comparable_name})"


class FileRef:
    def __init__(self, name:str, sha256hash:str) -> None:
        self.name = name
        self.sha256hash = sha256hash

    def test(self, check_hash) -> bool:
        raise NotImplementedError()

class FullFileRef(FileRef):
    def __init__(self, name: str, sha256hash: str, path: str) -> None:
        super().__init__(name, sha256hash)
        self.path = path

    def test(self, check_hash:bool = False) -> bool:
        if not os.path.exists(self.path):
            return False
        if check_hash:
            filehash = hashlib.sha256()
            with open(self.path, 'rb') as fh:
                while True:
                    data = fh.read(4096)
                    if data == b'':
                        break
                    filehash.update(data)
            if filehash.hexdigest() != self.sha256hash:
                return False
        return True

    def __str__(self) -> str:
        return f"{self.name} ({self.sha256hash}): {self.path}"





class Project:
    def __init__(self, name:str) -> None:
        self.name = name
        self.versions: dict[str, set[FileRef]] = {}
        self.files: dict[str,FileRef] = {}

    def add_file(self, version:Optional[str], file: FileRef):
        if version is not None:
            version_files = self.versions.get(version, set())
            version_files.add(file)
        self.files[file.name] = file

    def __iter__(self) -> "Iterator[FileRef]":
        return self.files.values().__iter__()

class Filter:
    def do_filter(self, project:str, version:str, filename:str) -> bool:
        raise NotImplementedError()

    def __call__(self, project:str, version:str, filename:str) -> bool:
        return self.do_filter(project, version, filename)

class ProjectNameRegexFilter(Filter):
    def __init__(self, filter:str) -> None:
        super().__init__()
        self.filter = re.compile(filter)

    def do_filter(self, project:str, version, filename) -> bool:
        return True if  re.search(self.filter, project) else False


def parse_files_txt(files_txt_path: str, filter: Optional[Callable] = None) -> "dict[PyPiPackageName, Project]":
    results: dict[PyPiPackageName, Project] = {}
    row_re = re.compile(r"^(.+?),(.+?),(.+?),([a-f0-9]{64})$")
    with open(files_txt_path) as fh:
        for line in fh:
            line = line.rstrip()
            matches = re.search(row_re, line)
            if matches is None:
                raise Exception(f"Error parsing: >{line}<")
            project, version, filename, filehash = matches.groups()
            if filter is not None and not filter(project, version, filename):
                # print(f"Skipping {project}")
                continue
            fileref = FileRef(filename, filehash)
            results.setdefault(PyPiPackageName(project), Project(project)).add_file(version, fileref)
    return results


def parse_web_dir(web_dir_path: str) -> "dict[PyPiPackageName, Project]":
    results:dict[PyPiPackageName, Project] = {}
    index_path = os.path.join(web_dir_path, "simple", "index.v1_json")
    with open(index_path, "rb") as fh:
        index_dict = json.load(fh)
    for project_dict in index_dict["projects"]:
        project = Project(project_dict["name"])
        project_files_path = os.path.join(web_dir_path, "simple", project.name, "index.v1_json")
        with open(project_files_path, "rb") as fh:
            project_files_dict = json.load(fh)
        for file_dict in project_files_dict["files"]:
            filename = file_dict["filename"]
            file_sha256 = file_dict["hashes"]["sha256"]
            file_url:str = file_dict["url"]
            assert file_url.startswith("../../packages/")
            file_relpath = urllib.parse.unquote(file_url)[len("../../"):]
            file_path = os.path.join(web_dir_path, file_relpath)
            file_ref = FullFileRef(filename, file_sha256, file_path)
            project.add_file(None, file_ref)

        results[PyPiPackageName(project.name)] = project
    return results

        


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--regex", help="Additional regext to apply to projects")
    parser.add_argument("-c", "--check-hashes", action="store_true", help="Also check file hashes")
    parser.add_argument("web_dir", help="Path the the \"web\" directory in your bandersnatch mirror")
    parser.add_argument("files_txt", help="Files to check for")
    
    args = parser.parse_args()
    check_hashes:bool = args.check_hashes

    print("Parsing CSV Packages...", end="", flush=True)

    filter = None
    if args.regex is not None:
        filter = ProjectNameRegexFilter(args.regex)


    csv_packages = parse_files_txt(args.files_txt, filter)
    print("Done")

    print("Parsing Bandersnatch Packages...", end="", flush=True)
    bandersnatch_packages =parse_web_dir(args.web_dir)
    print("Done")

    csv_package_names = set(csv_packages.keys())
    bandersnatch_package_names = set(bandersnatch_packages.keys())

    packages_missing_from_bandersnatch = csv_package_names - bandersnatch_package_names
    packages_only_in_bandersnatch = bandersnatch_package_names - csv_package_names

    packages_with_files_only_in_bandersnatch = set()
    for packagename in packages_only_in_bandersnatch:
        if len(bandersnatch_packages[packagename].files) > 0:
            packages_with_files_only_in_bandersnatch.add(packagename)

    packages_in_both = bandersnatch_package_names & csv_package_names

    print(f"There are {len(packages_missing_from_bandersnatch)} packages missing from bandersnatch")
    with open("bandersnatch_missing_packages.txt", "w") as fh:
        for missing_package in packages_missing_from_bandersnatch:
            print(missing_package, file=fh)
            print(f"{missing_package!r}")
    print(f"There are {len(packages_only_in_bandersnatch)} unexpected packages in bandersnatch")
    print(f"Of those {len(packages_with_files_only_in_bandersnatch)} have files")
    with open("bandersnatch_unexpected_packages.txt", "w") as fh:
        for unexpected_package in packages_with_files_only_in_bandersnatch:
            print(unexpected_package, file=fh)
            print(f"{unexpected_package!r}")
    print(f"There are {len(packages_in_both)} in both sets")

    missing_files:set[str] = set()
    unexpected_files:set[str] = set()
    files_to_check:set[FullFileRef] = set()

    for packagename in packages_in_both:
        bandersnatch_filenames = set(bandersnatch_packages[packagename].files.keys())
        pypi_filenames = set(csv_packages[packagename].files.keys())

        missing_files |= pypi_filenames - bandersnatch_filenames
        unexpected_files |= bandersnatch_filenames - pypi_filenames
        for file_to_check in bandersnatch_packages[packagename].files.values():
            assert type(file_to_check) == FullFileRef
            file_to_check = cast(FullFileRef, file_to_check)
            files_to_check.add(file_to_check)


    print(f"Preparing to check {len(files_to_check)} files")
    logger = ProgressLogger(len(files_to_check))
    fail_count = 0
    with open("bad_files.txt", "w") as fh:
        for file_to_check in files_to_check:
            if not file_to_check.test(check_hash=check_hashes):
                print(f"{file_to_check} is missing or corrupted", file=fh)
                fail_count += 1
            logger.report_item(f"Tested {file_to_check}")
    print(f"Completed checking files")
    print(f"There are {fail_count} missing or corrupted files")
    if fail_count > 0:
        print("See bad_files.txt")
    print(f"There are {len(missing_files)} files missing from bandersnatch")
    if len(missing_files) > 0:
        with open("bandersnatch_missing_files.txt", "w") as fh:
            for missing_file in missing_files:
                print(missing_file, file=fh)
        print("See bandersnatch_missing_files.txt")
    print(f"There are {len(unexpected_files)} unexpected files in bandersnatch")
    if len(unexpected_files) > 0:
        with open("bandersnatch_unexpected_files.txt", "w") as fh:
            for unexpected_file in unexpected_files:
                print(unexpected_file, file=fh)
        print("See bandersnatch_unexpected_files.txt")



if __name__ == '__main__':
    main()