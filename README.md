# Pulse of Melbourne Distributed Computing Project

 The Pulse of Melbourne Project harvests geospatial data from multiple social media platforms (Twitter, Flickr, FourSquare, Instagram, and Youtube) for the purpose of analysing location hotspots over time.  The motivation behind the project is to allow analysis of behavioural patterns in major cities, namely Meblourne and Sydney with possible applications being road and urban planning, identifying cultural communities, event marketing, semantic analysis, etc.
 
 # Compile Instructions:
 
      Clone the directory and then use maven to compile:
 
      $ mvn package
 
      A JAR file "pulse-jar-with-dependencies.jar" will be created in the 'out' directory.
 
 # Run Instructions:
 
      The application takes as command line arguments a city name (either melbourne or sydney) and a CouchDB database
      location in the form of IP:Port, for example 192.168.1.10:5984. After connectivity is confirmed, all
      necessary consumer threads are started.
      
      PLEASE ENSURE the 'log4j.properties' files and 'consumer.keys' files are in the same directory as the JAR file
      
      $ java -jar pulse-jar-with-dependencies.jar melbourne 192.168.1.10:5984
      or
      $ java -jar pulse-jar-with-dependencies.jar sydney 192.168.1.10:5984

      
      
