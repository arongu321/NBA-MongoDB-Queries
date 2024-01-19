# How to Use the MongoDB Examples Here
1. Run `./setup.sh <port number>` on the command line where the port number is used to connect to the mongoDB server
2. In a separate terminal window run `python3 examples.py <port number>` where the port number is the same port number from the 1st step to run the queries you run
3. Run `python3 answers.py <port number>` to view the expected outputs for each query

# Closing MongoDB Connection
- To shutdown mongoDB connection to server or restart it, run this command:
    `./reset.sh <port number>`