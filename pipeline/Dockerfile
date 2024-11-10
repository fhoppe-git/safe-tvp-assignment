# Use an official Python runtime as a parent image
FROM python:3.10

# Set the working directory in the container
WORKDIR /app

# Install any needed packages specified in requirements.txt
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install --no-cache-dir --trusted-host pypi.python.org -r requirements.txt

# Copy the Python script into the container
COPY . /app

# Run main.py when the container launches
ENTRYPOINT ["python"]
CMD ["main.py"]