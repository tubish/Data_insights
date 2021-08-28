## Introduction

Data Engineer project. Within this repository, everything you'll need has been provided.

## Requirements

The required library dependancies are within the requirements.txt file

## Setup

Run python3 generate_data.py to generate data

## Scenarios

You're a Data Engineer and have been provided with two datasets, a tickets.csv and customer.json file. 
The following insights are required to be written into an `/output` folder :

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

For each of these insights, a csv file with the results written 


