YouTube Channel Information Extraction and Analysis

This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel. The extracted data is stored in a MongoDB database, then migrated to a SQL data warehouse. The application enables users to search for channel details and join tables to view data in the Streamlit app.


Features

1. Extracts information on a YouTube channel using the Google API
2. Stores the extracted data in a MongoDB database
3. Migrates the data from MongoDB to a SQL data warehouse
4. Enables users to search for channel details
5. Provides the ability to join tables and view data in the Streamlit app

Usage

1. Open the Streamlit application in your browser
2. Enter the YouTube channel ID(s)
3. Click on the "Find" button to extract information from the YouTube channel
4. Click 'Store Data in MongoDB' button to store the fetched data to MongoDB database
5. Next from the multiselect dropdown select the channel(s) whose details you want to migrate to SQL Database
6. Click 'Migrate to SQL Database' to migrate 
7. After migrating, you can select your desired query from the query dropdown
8. Click the 'Get Report' button to get the result in the form of table

Contributing

Contributions are welcome! If you find any issues or have suggestions for improvement, please open an issue or submit a pull request.