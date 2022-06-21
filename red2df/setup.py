from setuptools import setup, find_packages


with open('README.md', encoding='utf-8') as f:
    long_description = f.read()
setup(
    name='red2df',
    version='1.0.4',
    packages=['red2df'],
    description='Create a Pandas dataframe with your Redshift data',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Hyunie',
    author_email='kyunghyun7843@gmail.com',
    url='https://github.com/Gyeong-Hyeon/pilot_works/tree/main/red2df',
    license='MIT',
    python_requires='>=3',
    install_requires=['pandas','redshift_connector']
)
