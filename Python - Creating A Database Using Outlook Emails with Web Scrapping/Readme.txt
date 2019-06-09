Objective: 
	- The objective is to scrape data within an OutLook email then add it to a .csv database.

The Solution: 
	- The first iteration i've created the process by creating advanced nested Excel formulas, however, the process still invovled manual steps. 
	- I fully automated the process by creating Python classes to apply formulas, sort, parse, clean and append the emails to a database.


Key Takeaways: 
	- I used Python inheritance classes to modularize and organize different components of the scraper.

Notes:
	- The model is located in the folder '(2) Model'
	- The outlook source data files have been removed for privacy reasons.
	- The main db has been removed for privacy reasons.
	- The pre-processing file is available. It is a batch job that would get loaded to the main db.
