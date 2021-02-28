import os
import shutil
"""
Purpose: This makes standing up a new Django Application with a Python Virtual Env. a breeze.

How to use: Adjust the django_app_name, project_name, and folder_path in the main function
and then run. 

Author: Chris Wilson 
Date: 2/28/21
"""


def make_dir(pn, fp, dj_an):
    """
    This joins the folder path and project name together to create a folder.
    It then changes to that drive and creates a python virtual env. Next it activates
    it, installs Djanjo, and then creates a project shell for Django. The folder
    structure is then changed to ensure ease of use.
    :param pn: Django app name
    :param fp: folder path for where you want the Django development app.
    :param dj_an: the project name
    :return:
    """
    path = os.path.join(fp, pn)
    os.mkdir(path)
    os.chdir(path)
    os.system(f'python -m venv {pn}_venv')
    os.system(f'{pn}_venv\\scripts\\activate.bat && pip install Django && django-admin startproject {dj_an}')
    shutil.move(f'{path}\\{dj_an}\\manage.py', f'{path}\\manage.py')
    shutil.move(f'{path}\\{dj_an}\\{dj_an}', f'{path}\\_{dj_an}')
    shutil.rmtree(f'{path}\\{dj_an}')
    os.rename(f'{path}\\_{dj_an}', f'{path}\\{dj_an}')


if __name__ == '__main__':
    django_app_name = 'Sample_Application_Name'
    project_name = "Django_Deployment_Project"
    folder_path = r"E:\Python Code Repo\Django"
    make_dir(project_name, folder_path, django_app_name)

