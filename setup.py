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
      packages=['SurveyGizmo','ReleaseCalendar','ReleaseCalendar','Kitsune','GoogleAnalytics','Twitter','Product_Insights'],
      package_data={"GoogleAnalytics": ["urls_*.txt"]},
      install_requires=[
          'requests',
          'six<2.0.0dev,>=1.13.0',
          'grpcio>=1.8.2',
          'google-cloud-logging',
          #'google-cloud-storage==1.26.0',
          'google-cloud-storage',
          #'google-resumable-media<0.6dev,>=0.5.0',
          'google-resumable-media',
          'google-cloud-translate',
          'google-cloud-language',
          'pandas',
          'nltk',
          'tweepy',
          'oauth2client',
          'google-cloud-bigquery',
          'google-api-core',
          'google-api-python-client',
          'SurveyGizmo',
          'gcsfs',
          'cython',
          'pyarrow==0.9.0',
          'python-dateutil',
          'PySocks',
          'pytrends'
      ],
      test_suite='nose.collector',
      tests_require=['nose', 'nose-cover3'],
      entry_points={
          'console_scripts': [
              'run-surveygizmo=SurveyGizmo.run_get_survey_data:main',
              'run-kitsune=Kitsune.get_kitsune_data:main',
              'run-googleanalytics=GoogleAnalytics.get_ga_data:main',
              'run-twitter=Twitter.get_twitter_data:main',
              'run-analyze-twitter=Twitter.analyze_twitter_data:main',
              'run-releasecalendar=ReleaseCalendar.get_release_calendar:main',
          ],
      },
      include_package_data=True,
      zip_safe=False)

