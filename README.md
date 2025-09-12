# Introduction 
Developed CI/CD pipeline frameworks for ELT processes based on client data sources and requirements.
# Getting Started
In this project, the tools majorly used are: Snowflake, Pyspark, Apache Airflow, Azure Devops, SQL Server
Snowflake - For Loading and storing the data. It has data of different versions for each time updating the record (history of data) and also it has raw data stored in staging or vairant table and the meaningful data (data mapped with proper fields) in the target table. The business client has the visibility/access to the views (only specific data visible based on the business requirement) For moving the data from the variant to target target, I have used streams, tasks and procedures as part of the snowflake objects.
The framework automatically produces the DDL scripts for snowflake objects.

Pyspark - This is what is used for coding to build the framework.

Apache Airflow - The jobs are enabled to run to process the flow of extracting the data from the client source path to transfer and load to the snowflake staging table. All the jobs are being scheduled to run on certain time based on business requirements. The job fails if there is some exceptions raised during the process. All the information, warnings, errors details logged in the airflow.

Azure Devops - This is used to maintain the respository. Build and deploy the latest code to the production.

SQL Server - It has the config setup for each framework. And the log details of the files in each pipelines are also stored - maintaining the history of files being processed. I has also the data from the client stored. Those data is also replicated in snowflake. So, copy command text has the query what supposed to be replicated in the snowflake from sql server.

# Build and Test
For testing - WinScp, SavyintCloud, Visual Studio and Azure Devops are used.

SavyintCloud generates password which should be used for changing the code in WinScp and for deploying the latest version in Azure Devops (Service connections in project settings)

# Contribute
TODO: Explain how other users and developers can contribute to make your code better. 

If you want to learn more about creating good readme files then refer the following [guidelines](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops). You can also seek inspiration from the below readme files:
- [ASP.NET Core](https://github.com/aspnet/Home)
- [Visual Studio Code](https://github.com/Microsoft/vscode)

- [Chakra Core](https://github.com/Microsoft/ChakraCore)
