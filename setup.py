import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()
with open('requirements.txt') as f:
    required = f.read().splitlines()

setuptools.setup(
    name='susanin',
    version='1.2.2',
    author='INID',
    author_email='m.vasilevskaia@cpur.ru',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/CAG-ru/susanin',
    project_urls={
        "Bug Tracker": "https://github.com/CAG-ru/susanin/issues"
    },
    license='Apache',
    include_package_data=True,
    package_data={'ovrazhki': ['ovrazhki/*.csv'], 'geonorm': ['geonorm/*.json', 'geonorm/*.csv', '*.json']},
    packages=['ovrazhki', 'geonorm', 'geonorm.nat_new', 'geonorm.nat_new.grammars'],
    install_requires=required
)
