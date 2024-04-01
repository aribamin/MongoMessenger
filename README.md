[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/Fozs_Okj)
# CMPUT 291 Mini Project 2 - Winter 2024  
Group member names and ccids (3-4 members)  
  jgourley, Jared Gourley. <br />
  dricmoy, Dricmoy Bhattacharjee.  <br />
  arib1, Arib Amin. <br />
  jennif11, Jennifer Wang <br />

# Group work break-down strategy - Started: 23 March, 2024 || Completion: April 1, 2024
Our group adopted a collaborative approach where we divided the tasks based on individual strengths and interests. Here's a breakdown of the work items among partners:

Arib <br />
Wrote initial queries for Task 1 Step 3 <br />
Wrote indices code for Task 1 Step 4
Writing report and checking specifications for Task 1

Estimated Time Spent: <br />

Progress: Completed all tasks assigned

Dric <br />
Contributed to group strategy and crafted a tentative README.md file <br />

Estimated Time Spent: <br />
Progress: Completed all tasks assigned

Jared <br />
Initialized necessary repo files

Estimated Time Spent: <br />
Progress: Completed all tasks assigned <br />

Jen <br />

Estimated Time Spent: <br />
testing queries debug (2 hours) 
Progress: Completed all tasks assigned  <br />

# Method of Coordination 
We maintained communication primarily through a Discord group chat, where we regularly updated each other on progress, discussed any roadblocks, and coordinated the integration of individual components. Additionally, we held virtual meetings on discord to synchronize our efforts and address any issues collectively. <br />

Initial Tasks:
Ensure all members have joined the GitHub repository for mini project 2, Completition: March 27th, 2024
Create the necessary starting files into the repo - Jared <br />
Update the readme file timelines, remind group members to discuss their contributions in the discord group chat to keep a record of time as well as work done- Dric <br />

Task 1 - Started: 23 March, 2024 || Completition: 29th March, 2024<br />
- Set up the collections and having the documents inserted from the json files (steps 1 and 2) - Jared <br />
- Step 3: Writing the queries needed - Arib <br />
- Step 3.5: Debug queries - Jennifer <br />
- Step 4: Creating indices and reprinting- Arib, Dric <br />
- Final docstrings and outputs - Dric <br /> 

Testing for Task 1 <br />
- Create databases on different ports <br />
- Shows proper usage, if Python script is run improperly <br />

Task 2 - Started 26 March, 2024 || Compleition: 1 April, 2024 <br />
- Step 1: Set up the collections and having the documents inserted from the json files- Arib, Dric <br />
- Step 2: Writing the queries needed - Arib, Jen <br />
- Step 2.5: Debug queries - Dric, Arib <br />
- Final docstrings and outputs - Dric <br /> 

Readme.md with all updates - Dric
- List names and ccids of all group members
- No collaborators
- No Ai tools
- Group Work break-down strategy, time spent and progress made
- Steps required to run code

report.pdf - Jared, Arib, Jen, Dric
-General overview <br />
-User guide (+ code instructions) <br />
-Handling large json files strategy <br />
-paste query outputs <br />
-whatever explanations apply for explaining why the runtimes change (because of indices) <br />
-discussion of if embedded or not is better for this schema? <br />
-(double check anything else it asks) <br />

# Code execution guide
1. Clone this repository to your local machine. <br />
2. Ensure you have Python installed along with the necessary libraries (such as pymongo). <br />
3. Ensure the mongodb server is running in the background using mongod --port {any_open_port} --dbpath {folder of choice} & <br />
4. Run the provided Python scripts, with the appropriate command-line arguments as specified in the project guidelines. <br />
5. Ensure you run a build script and wait till it outputs Time taken before executing a query script. For example, run "python3 task1_build.py {port_number}", time taken will output; then run "python3 task2_query.py {same_port_number}" <br />
6. Refer to the report for a comprehensive overview of the system, user guide, and analysis of query runtimes. <br />

Note
Please ensure that all required files and configurations are set up according to the project specifications to ensure smooth execution of the code. If you encounter any issues, refer to the report or reach out to the group members for assistance. <br />

# AI Agents
We did not utilize any AI tools for this project.

# Collaborations
Names of anyone you have collaborated with (as much as it is allowed within the course policy) or a line saying that you did not collaborate with anyone else.  
We declare that we did not collaborate with anyone else.
