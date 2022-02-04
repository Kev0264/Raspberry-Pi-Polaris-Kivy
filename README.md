# Polaris Raspberry Pi Program

This is the operator interface for the Polaris device. 

These commands assume that you have python3 and pip3 installed. To check if python3 is installed, run the following command:
```
python3 -m pip --version
```

To get everything working, run the commands below to setup and run the project. This was created in a Linux environment. If running on Windows, the main difference should only be that instead of `cp` you would use `copy` (unless you're using PowerShell, in which case either works). First we will create a virtual environment *env* then activate it. Then from within the virtual environment we will install the required pip modules, make sure our environment variables are set, and then run the application.

This requires a database to be running with either MySQL or MariaDB. The connection parameters are defined in polarisdb.py. As long as the database exists, the tables will be automatically created. If they are modified, the changes will need to be made manually.

```
python3 -m venv env
source env/bin/activate
pip3 install -r requirements.txt
cp .env.example .env
python3 main.py
```
