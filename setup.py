from setuptools import setup


# start develop mode:
# >>> python setup.py develop
# end develop mode:
# >>> python setup.py develop --uninstall

# install:
# pip install ./customp
# uninstall:
# pip uninstall customp


setup(
    name='customp',
    version='0.1.0',
    description='Custom multiprocessing fucntions',
    author='Mathew Huerta-Enochian',
    author_email='mathewhe@gmail.com',
    url='https://github.com/mathewhuen/customp',
    packages=[
        'customp',
    ],
)
