from setuptools import setup, find_packages

setup(
    name='red2df',
    version='1.0.0',
    packages=['red2df'],
    description='Create a Pandas dataframe with your Redshift data',
    author='Hyunie',
    author_email='kyunghyun7843@gmail.com',
    url='https://github.com/Gyeong-Hyeon/pilot_works/tree/main/redshift_conn',
    license='MIT',
    python_requires='>=3',
    install_requires=['pandas','psycopgy2']
)
