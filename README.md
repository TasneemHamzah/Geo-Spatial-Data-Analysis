# Geo-Spatial-Data-Analysis


Apache Spark is an analytics platform used to process large amounts of data, supporting APIs for many common programming languages, such as Python, Java and Scala, as well as the ability to run SQL-like queries. It is thus well-suited to many large-scale modern analytics applications (Apache, Spark Overview).

Geospatial data analysis is a particularly data intensive domain due to the high dimensionality of the data (coordinates of a specific location, time, and other attributes) and rapid creation, whether from sensors taking measurements at specified time intervals, or data from mobile device applications. This scale makes Apache Spark a useful tool for processing such data.

In this project, I implement code using Scala and Apache Spark SQL to complete a geospatial hotspot analysis of taxi trips in New York City.

Hot zone analysis: The first task requires us to take as input a list of points representing New York City taxi rides, given by latitude and longitude coordinates, and a list of rectangles, given by four geographic coordinates mapping the bounds of the rectangle. We then perform a join on the two lists and output an intermediate data frame of tuples of the form (rectangle coordinates, point coordinate), such that a tuple exists for each rectangle, point combination where the point is contained within the rectangle. We implement this portion of the task using a Spark SQL call and the ST_Contains function specified above. The Spark SQL call then joins the data and returns tuples where ST_Contains returns true.

Finally, we apply a groupBy and .count operation to obtain a data frame with each rectangle and the corresponding count of points. This gives the heat of each rectangle, indicating the density of taxi ride pickups that occurred within that space. In the last step we use Spark SQL to sort the data frame in ascending order of rectangle coordinates before returning the final sorted data frame.

Hot cell analysis: For the second task, we create a grid given the taxis data and calculate the Getis-Ord statistic for each cell, which indicates the density of clustering within. We then output the fifty “hottest” cells, sorted in descending order of the Getis-Ord statistic, described here in detail by Esri (n.d.), How High/Low Clustering (Getis-Ord General G) Works: https://pro.arcgis.com/en/pro-app/latest/tool-reference/spatial-statistics/h-how-hot-spot-analysis-getis-ord-gi-spatial-stati.htm.

The “grid” is a cube of three dimensions: longitude, latitude, and time. The size of a cell is 0.01 degrees latitude and longitude, and one day of time.

Once we define the cell dimensions, number of cells and min and max value for each dimension, we determine whether a given point lies within a cell with the help of the ST_Within function, defined above, but adapted for three dimensions.

def ST_Within(px1:Int, py1:Int, pz1:Int, px2:Int, py2:Int, pz2:Int)


We then define a function to calculate the Getis-Ord statistic for each cell. Once the Getis-Ord statistic is calculated, we then create a data frame grouping each cell by its x, y, and z coordinate values, and sort in descending order based on the Getis-Ord statistic. Finally, we output the top fifty cells in the data frame, which represent the fifty “hottest,” or most dense cells (combination of location and day).

