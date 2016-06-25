from pathlib import Path as P
import hashlib

from os import path
import os

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Folders, CheckObjs, HashTable, HashCount

engine = create_engine('sqlite:///data1.db')
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

print(P.cwd())


def get_file_hash(fname):

    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()


def get_folder_set(limit=100):
    folders_list = session.query(Folders.folder_path, Folders.id, Folders.check_count).filter(Folders.check_count <= 0).limit(limit)
    folders_list = folders_list.all()
    output = []
    for i in folders_list:
        i = Folders(folder_path=i[0], id=i[1], check_count=i[2])

        output.append(i)
    return output


def find_files(starting_point):
    now = datetime.utcnow()
    root = P(starting_point)
    item_list = []
    folder_contents = root.iterdir()
    y = 0

    for folder_content in folder_contents:
        file_name = P(folder_content)

        if file_name.is_file():
            x = file_name.resolve()
            name = file_name.name

            name = name.split('.')

            try:
                ext = name[1]
            except IndexError:
                ext = 'None'

            name = name[0]
            item = CheckObjs(file_name=name,
                             file_ext=ext,
                             full_path=str(x),
                             added_date=now)
            item_list.append(item)
            y += 1
    session.add_all(item_list)
    session.commit()
    return y


def add_folders(folder_list):
    now = datetime.utcnow()
    for folder in folder_list:
        entry = Folders(folder_path=folder, added_date=now)
        session.add(entry)
    session.commit()


def folder_checked(when, folder):
    try:
        folder.check_count += 1
    except AttributeError:  # FIXME this realy needs to be fixed
        folder.check_count = 1

    folder.last_checked = when
    return folder


def walk_path(start):
    path_folders = []
    y = 0
    for root, dirs, files in os.walk(start):
        for dire in dirs:
            x = path.join(root, dire)
            path_folders.append(x)
            if len(path_folders) == 100:
                add_folders(path_folders)
                y += len(path_folders)
                path_folders = []

    add_folders(path_folders)
    y += len(path_folders)

    return y


if __name__ == '__main__':
    start_point = datetime.utcnow()
    # here = '/home/boomatang/temp/project-lib/check'
    here = '/home/boomatang/Projects'
    # here = '/home/boomatang/Documents'
    y = walk_path(here)
    print(y)

    cycle = 0
    folder_count = session.query(func.count(Folders.id)).scalar()
    get_count = 200
    count = 0

    print(folder_count)
    while cycle <= folder_count:
        now = datetime.utcnow()
        print('Add files cycle = ' + str(cycle))
        content = get_folder_set(limit=get_count)
        # TODO put threads or something here

        for con in content:
            count += find_files(con.folder_path)
            item = folder_checked(now, con)
            session.query(Folders). \
                filter(Folders.id == item.id). \
                update({Folders.check_count: item.check_count, Folders.last_checked: item.last_checked},
                       synchronize_session=False)
        session.commit()
        cycle += get_count

    print('Files found: ' + str(count))

    # checking files
    file_count = session.query(func.count(CheckObjs.id)).scalar()
    cycle = 0
    count = 0
    while cycle <= file_count:
        now = datetime.utcnow()
        print('Files hashed cycle = ' + str(cycle))

        all_files = session.query(CheckObjs).filter(CheckObjs.check_count == 0).limit(get_count)
        test = []
        for i in all_files:
            i.checksum = get_file_hash(i.full_path)
            i.check_count += 1
            i.last_checked = now
            test.append(i)
            count += 1

        session.add_all(test)

        session.commit()
        cycle += get_count
    print('Files checked: ' + str(count))

    # This is the hash count

    query = session.query(CheckObjs.checksum, func.count(CheckObjs.id)).group_by(CheckObjs.checksum)
    records = query.all()

    hash_count_id = 1
    for i in records:
        hash_entries = []
        if i[1] > 1:
            item = HashCount(checksum=i[0], count=i[1])
            hash_entries.append(item)
            hash_count_id += 1
        session.add_all(hash_entries)
    session.commit()

    # add the ids to the hash table

    query = session.query(HashCount)
    result = query.all()

    for i in result:
        log_items = []
        items = session.query(CheckObjs).filter(CheckObjs.checksum == i.checksum).limit(i.count)
        items = items.all()
        for item in items:
            log = HashTable(file_id=item.id, hash_id=i.id)
            log_items.append(log)
        session.add_all(log_items)
    session.commit()
