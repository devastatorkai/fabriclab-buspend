
Step 1: Login to Microsoft Fabric, (https://app.powerbi.com/) using your allocated team username and password.

Step 2: Navigate to your assigned workspace which is the same as your login number.

**LAB 01: Data Streams**

Step 1: Create a folder called Lab 01- Streams

https://github.com/user-attachments/assets/c38c7d65-e924-442c-8be0-420231a2665c

Step 2: Create an Event Stream

In the next step we create a black event stream named "EventStreamxx" replace xx with your login number or workspace number. Thereafter create a customEndpoint-Source this will be used in the next step. And publish the stream.

https://github.com/user-attachments/assets/7dca3a1e-82e7-45f2-9e14-55bdf94a839d

Step 3: Data Emulator

Download the following file and upload it to the folder Lab 01 - Streams to generate some sample data streams for us. Once the file is uploaded edit the eventstream name in the code block 3 line 4 to the name of your event stream. 

https://github.com/devastatorkai/fabriclab-buspend/blob/UserLabs/BUSpendEmulator.ipynb

https://github.com/user-attachments/assets/788cb127-cd4d-41a1-ab44-0d2738b578c6

Step 4: Running the Emulator

Once the Event Stream is setup correctly click the Run all Button and scroll to the end to check that the events are being processed.

https://github.com/user-attachments/assets/c9249e24-7475-4230-bb30-9c4854368313

Step 5: Creating an EventHouse for the Stream

The streaming data needs to be stored in a Time Series database called an Event House, in this next step we will create on in the Eventstream. Name the Eventhouse EvenHousexx where xx is the number of your login or workspace.

First go to the Event Stream and click on refresh to ensure you getting events streamed. Then go to edit mode. 

https://github.com/user-attachments/assets/93a008ef-61f2-40e7-927e-b89d671d65fe

Next Add the EventHouse as a destination and create it. You can name the table BUSpend or anything you would like. Once done **Publish** your changes.

https://github.com/user-attachments/assets/486480bf-c580-460a-b1fe-7dc2d846c2e8

**LAB 02: Creating a Real-time Dashboard**

Step 1: Navigating the Event House

Find the newly created eventhouse in your workspace and open it up, click on the KQL database the the table you created, navigate around the database and check the data stream coming in.

https://github.com/user-attachments/assets/84917717-047d-43e4-8917-8f1a7b0fa253

Step 2: Creating a Real-time Dashboard

Next we create a real time dashboard, the sky is the limit on this and use the copilot if you dont know how to write KQL statements.

https://github.com/user-attachments/assets/4ecd51e9-f95b-41e0-9c3e-e0ff54e24077

Step 3: Using Activator

Create a new folder Lab 02 - Real Time. Go back to the Event Stream you created in Lab 1. And click on Set Alert in the menu bar. 

https://github.com/user-attachments/assets/e7df3ae4-6e79-492f-a652-b1c4bb055a9c

Fill in all the required information and click on Create. Once Created click on the Open button and check your Data Activator at work.

https://github.com/user-attachments/assets/d7af9bbe-fdd5-46fe-8d26-685993ff34fb

**LAB 03: Build a Lakehouse**

Step 1: Create a new folder called Lab 03 - Lakehouse. 

Download the LakehouseLoader Notebook. And upload it to Fabric Workspace. This notebook contains code to create a new Lakehouse and build a few tables with sample data. 

https://github.com/user-attachments/assets/6bc28a04-be47-4295-a1f0-648d65fc7588

Run the 1st Cell to Create the lakehouse then attach that lakehouse to the current notebook before running the next cell.

https://github.com/user-attachments/assets/315bc298-18aa-41af-b21a-e5b45a5a2290

Once completed Navigate to your new Lakehouse with the newly created tables. From here you can now create semantic models, query the data, create reports or build data agents.

https://github.com/user-attachments/assets/df2207bc-8561-4086-a3dd-318969750d02

**LAB 04: Create a Data Agent**

Step 1: Create a folder called Lab 04 - Data Agent, and select new item to create a data agent. Give the agent any name you wish. Once created add the lakehouse created in the last lab to your agent.

https://github.com/user-attachments/assets/615f70fd-035d-4a2a-a2d5-4677459cc8ae

Select all the tables from your source, head to setup and give your agent some instructions. Click Add tools and select code interpreter.

https://github.com/user-attachments/assets/a84a3313-77f4-47e7-9189-faba8d66c220

Now ask your agent any questions to test it out, ask the agent to create a bar graph or pie chart. Dont forget to Publish your changes.

https://github.com/user-attachments/assets/1ad953a1-8375-4df2-a176-7b3c24353d09



