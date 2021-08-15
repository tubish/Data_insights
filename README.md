## Introduction

Data Engineer project. Within this repository, everything you'll need has been provided.

## Requirements

The Repo provides a Dockerfile that contains everything needed to work with pyspark. However, if you're not comfortable with Docker, feel free to install the required tools on your machine.

* Python
* Java (For Spark)

The required library dependancies are within the requirements.txt file

## Setup

Run the /generate_data.py script to create your datasets within the `/data/` directory. e.g.

    docker-compose run two_circles python3 generate_data.py

Or without Docker:

    python3 generate_data.py

## Run your code

If using Docker, the following can be used to run a command, from the root directory:

    docker-compose run two_circles python3 -m python-assessment.ticket_processing_service.assessment

Or without Docker: 

    python3 -m python-assessment.ticket_processing_service.assessment

## Scenarios

You're a Data Engineer at Two Circles, and have been provided with two datasets, a tickets.csv and customer.json file. The person who has provided them to you would like the following things:

1. A table of Events with formatted dates and count of Orders
2. Tickets by Customer Title, ordered by Quantity
3. For each Customer, a list of Events
4. List of **all** Customers with an additional column called "MultiEvent", set to `True` for those Customers with more than 1 Event
5. Largest Order by Quantity for each Customer
6. Second Largest order by Quantity for each Customer
7. Gap/Delta in Quantity between each Customers Order
8. A Spark SQL Table containing all provided data in a denormalized structure, ordered by Event date
9. Create a Data Model, with a Transaction, Customer and Event table, providing a way to join the 3 tables
10. Net Sales by Season, with percentage comparison to the previous Season
11. Find the latest event purchased by customer, and depending on what season the event date falls in assign the status of 'Current Ticket Purchaser', 'Previous Ticket Purchaser' or 'Future Ticket Purchaser'.

For each of these insights, a csv file with the results written into an `/output` folder


