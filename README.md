# CoronaGram
Data Mining Project as part of an ITC Data Science course 

Our program scrap instagram page of a specific hashtag and
take all post link related to this hashtag.
After that it scraps each post page and print the result
as a data table (possibility to choose output format: csv,
sql, pkl).

The program is optimized to work with different CPUs in 
parallel. Quantity of posts for Corona hashtag for example
is huge, more than 20 millions posts, so optimization
is essential.

It used selenium library for scrolling on page
because instagram has dynamic pages. Selenium headless 
option permits to limit usage of graphical resources.

Argument call permits the user to choose:
1) hashtag to scrap (example: corona)
2) output format (choice: csv, pkl, sql)
3) number of CPUs to use (default: all)
4) number of posts to scrap (default: 1)
5) column to print (example: post_url, #like, location...)
6) browser to use (default: chrome)
