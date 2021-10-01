import setuptools

setuptools.setup(
    name="googlecloudutils",
    author="Mario Lamberti",
    package=setuptools.find_packages(),
    install_requires=['google-cloud-pubsub', 'google-cloud-storage', 'pandas'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ]
)