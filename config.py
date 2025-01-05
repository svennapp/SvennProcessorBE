import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    # Flask configuration
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key'

    # Database configuration
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
                              'sqlite:///' + os.path.join(basedir, 'app.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Jobs folder path
    JOBS_FOLDER = os.path.join(basedir, 'app', 'jobs')