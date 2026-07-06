
Step 1: Login to Microsoft Fabric, using your allocated team username and password.

Step 2: Navigate to your assigned workspace

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

Next Add the EventHouse as a destination and create it. You can name the table BUSpend or anything you would like. Once done Publish your changes.

https://github.com/user-attachments/assets/486480bf-c580-460a-b1fe-7dc2d846c2e8



