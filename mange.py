from pathlib import Path as P
from os import path
import os

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Folders, CheckObjs

engine = create_engine('sqlite:///data1.db')
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

print(P.cwd())


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
    for root, dirs, files in os.walk(starting_point):
        for file_name in files:
            x = path.join(root, file_name)
            name = file_name.split('.')

            try:
                ext = name[1]
            except IndexError:
                ext = None

            name = name[0]

            item = CheckObjs(file_name=name,
                             file_ext=ext,
                             full_path=x,
                             added_date=now)
            session.add(item)

    session.commit()


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
    here = '/home/boomatang/Projects'
    y = walk_path(here)
    print(y)

    cycle = 0
    folder_count = session.query(func.count(Folders.id)).scalar()
    get_count = 50

    print(folder_count)
    while cycle <= folder_count:
        now = datetime.utcnow()
        print('cycle = ' + str(cycle))
        content = get_folder_set(limit=get_count)
        # TODO put threads or something here

        for con in content:
            find_files(con.folder_path)
            item = folder_checked(now, con)
            session.query(Folders). \
                filter(Folders.id == item.id). \
                update({Folders.check_count: item.check_count, Folders.last_checked: item.last_checked},
                       synchronize_session=False)
        session.commit()
        cycle += get_count
