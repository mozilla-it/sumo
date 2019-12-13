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
      install_requires=[
          'requests==2.22.0',
          'grpcio>=1.8.2',
          'google-cloud-logging==1.11.0',
          'google-cloud-storage==1.16.1',
          'google-cloud-translate==1.5.0',
          'google-cloud-language==1.2.0',
          'pandas==0.25.0',
          'nltk==3.4.5',
          'tweepy',
          'oauth2client==4.1.3',
          'google-cloud-bigquery==1.16.0',
          'google-api-core==1.13.0',
          'google-api-python-client==1.7.9',
          'six==1.12.0',
          'SurveyGizmo',
          'gcsfs',
          'python-dateutil==2.8.0',
          'pytrends==4.7.0'
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

