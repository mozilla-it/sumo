from setuptools import setup
import os

def readme():
    with open('README.md') as f:
        return f.read()


setup(name='sumo',
      version='0.2',
      description='Scripts for SUMO data access',
      long_description=readme(),
      keywords='sumo surveygizmo kitsune twitter reviews',
      url='https://github.com/mozilla-it/sumo',
      author='Nancy Wong',
      author_email='nawong@mozilla.com',
      license='...',
      packages=['SurveyGizmo','GooglePlaystore','ReleaseCalendar','Kitsune','GoogleAnalytics','Twitter'],
      install_requires=[
          'requests',
          'grpcio>=1.8.2',
          'google-cloud-logging',
          'google-cloud-storage',
          'google-cloud-bigquery',
          'google-api-core',
          'SurveyGizmo',
          'fs-gcsfs',
          'python-dateutil',
      ],
      test_suite='nose.collector',
      tests_require=['nose', 'nose-cover3'],
      entry_points={
          'console_scripts': [
              'run-surveygizmo=SurveyGizmo.run_get_survey_data:main',
              'run-kitsune=Kitsune.get_kitsune_data:main',
          ],
      },
      include_package_data=True,
      zip_safe=False)

