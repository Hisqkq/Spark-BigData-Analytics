# Wikipedia Image Analysis Project - Big Data Final Project

## Overview

This project was completed as part of the Big Data course at Radboud University. The course focused on leveraging big data technologies and distributed computing to analyze massive datasets. The final project involved analyzing a 700 GB web archive (WARC) segment from 2016, stored on the university's cluster with HDFS and other big data tools. The primary goal of this project was to perform an in-depth analysis of the images present in Wikipedia pages within the archive, including statistics on image sizes and identifying the largest image.

## Objectives

- **Analyze Image Data**: Extract and analyze images from Wikipedia articles contained in the 700 GB WARC archive.
- **Image Size Statistics**: Calculate and present detailed statistics on the sizes of images found within the dataset.
- **Identify the Largest Image**: Locate the largest image in terms of file size within the entire 700 GB dataset.

## Tools and Technologies

- **Language**: ***Scala*** was the primary programming language used for this project, chosen for its compatibility with big data tools such as Apache Spark.
- **Cluster Computing**: The analysis was conducted on the Radboud University cluster, utilizing ***HDFS*** for distributed storage and ***Apache Spark*** for processing large datasets.
- **WARC Format**: The dataset consisted of web archives in the ***WARC (Web ARChive)*** format, which is commonly used for storing web crawls.
- **Apache Zeppelin**: Some of the project assignments were conducted using Apache Zeppelin notebooks for interactive data analysis.

## Project Workflow

1. **Initial Analysis on a Single WARC File**:
   - We began by focusing on a single WARC file to develop and test our Scala program.
   - The goal was to extract images from the WARC file, calculate their sizes, and perform preliminary statistical analysis.

2. **Scaling Up to Multiple WARC Files**:
   - After successfully processing a single WARC file, the analysis was extended to multiple WARC files.
   - The program was refined to handle larger datasets efficiently, taking advantage of the distributed computing capabilities of the cluster.

3. **Full Dataset Analysis (700 GB)**:
   - Finally, the program was scaled to analyze the entire 700 GB of WARC files.
   - The analysis was conducted in parallel across the cluster, and the resulting data was aggregated to produce comprehensive statistics on all images found in Wikipedia.
   - We successfully identified the largest image within the dataset.

## Project Report and Documentation

- **Final Report**: Detailed information about the project, including methodology, results, and conclusions, can be found in the final project report: `Final project/Project_report_Lavandier_Théo.pdf`.
- **Zeppelin Notebooks**: Some of the project assignments and preliminary analyses were conducted using Apache Zeppelin notebooks. These notebooks are also included in the project repository.


## Conclusion

This project demonstrated the power of distributed computing for big data analysis. By leveraging Scala and Apache Spark on a Hadoop cluster, we were able to efficiently process a massive 700 GB dataset and extract meaningful insights about the images in Wikipedia. The successful identification of the largest image and the comprehensive statistics generated provide valuable information on the nature of visual content in Wikipedia.

For more detailed information, please refer to the project report and Zeppelin notebooks included in this repository.

