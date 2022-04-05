import json
import uuid
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import boto3
import pandas as pd
import time
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Flights')



def lambda_handler(event, context):

    options = Options()
    options.binary_location = '/opt/headless-chromium'
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--single-process')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome('/opt/chromedriver',chrome_options=options)

    arrivals = ["ATL","LAX","HND","LHR","LIN","CDG","AMS","HKG","DXB","BCN","DBV","ATH","PRG","BUD","TLV","ZRH","CAI","KEF","LIS","DPS","BKK"]
    departure = "KRK"
    today = datetime.datetime.now().date()
    
    class_types = ["economy", "business", "first"]
    one_way_flight = "oneway"
    dates = str(pd.date_range(today, today + datetime.timedelta(days=7)))

    for arrival in arrivals:
        for class_type in class_types:
            for date in dates:
                URL = f"https://www.esky.pl/flights/select/{one_way_flight}/ap/{departure}/ap/{arrival}?departureDate={date}&pa=1&py=0&pc=0&pi=0&sc={class_type}"
                driver.get(URL)
                
                airlines = []
                plane_changes = []
                prices = []
                last_seats = []
                flight_times = []
                departure_times = []
                arrival_times = []
                next_day_arrivals = []
    
                try:
                    allFlights = driver.find_element(by=By.CLASS_NAME, value="offers")
                    flights = allFlights.find_elements(by=By.TAG_NAME, value="li")
    
                    for flight in flights:
                        price = flight.find_element(by=By.CLASS_NAME, value="price-bold")
                        prices.append(price.text)
    
                        plane_change = flight.find_element(by=By.CLASS_NAME, value="connecting-info")
                        plane_changes.append(plane_change.text)

    
                except NoSuchElementException:
                    table.put_item(Item={
                        'id':str(uuid.uuid4()),
                        'departureAirport': departure,
                        'arrivalAirport': arrival,
                        'date': date,
                        'prices': "NO FLIGHT",
                        'flightTimes': "NO FLIGHT",
                        'departureTimes': "NO FLIGHT",                        
                        'arrivalTimes': "NO FLIGHT",
                        'nexDayArrivals': "NO FLIGHT",
                        'lastSeats': "NO FLIGHT",
                        'planeChanges': "NO FLIGHT",
                        'airlines': "NO FLIGHT",
                        'classType': class_type,
                        'timestamp': str(today)
                    })
                    pass
    
                for num in range(1, 11): 
                    try:
                        airline = driver.find_element(by=By.XPATH,
                                                          value=f'/html/body/fsr-app/fsr-flights-search-result/fsr-qsf-layout/section/div/flights-list/div/div[2]/div/div/ul/li[{num}]/esky-flight-offer-group/div[1]/div[2]/div/leg-group/div/header/div/airline-logo[2]/div/img')
                        airline_html = airline.get_attribute("outerHTML")
                        airlines.append(airline_html.split('alt')[1].split('="')[1].split('"')[0])
                    except NoSuchElementException:
                        try:
                            airline = driver.find_element(by=By.XPATH,
                                                              value=f"/html/body/fsr-app/fsr-flights-search-result/fsr-qsf-layout/section/div/flights-list/div/div[2]/div/div/ul/li[{num}]/esky-flight-offer-group/div[1]/div[2]/div/leg-group/div/header/div/airline-logo[1]/div[1]/img-fallback/img")
                            airline_html = airline.get_attribute("outerHTML")
                            airlines.append(airline_html.split('alt')[1].split('="')[1].split('"')[0])
                        except NoSuchElementException:
                            pass
    
                    try:
                        last_seat = driver.find_element(by=By.XPATH, value=f"/html/body/fsr-app/fsr-flights-search-result/fsr-qsf-layout/section/div/flights-list/div/div[2]/div/div/ul/li[{num}]/esky-flight-offer-group/div[1]/div[2]/flight-offer-price-info/div/div[2]/span/span")
                        try:
                            last_seat_html = last_seat.text
                            last_seats.append(last_seat_html)
                        except StaleElementReferenceException:
                            last_seats.append("")
                    except NoSuchElementException:
                        last_seats.append("")

                    try:
                        departure_time = driver.find_element(by=By.XPATH, value=f"/html/body/fsr-app/fsr-flights-search-result/fsr-qsf-layout/section/div/flights-list/div/div[2]/div/div/ul/li[{num}]/esky-flight-offer-group/div[1]/div[2]/div/leg-group/div/div/div/span[1]")
                        departure_time_html = departure_time.text
                        departure_times.append(departure_time_html)
                    except NoSuchElementException:
                        pass

                    try:
                        arrival_time = driver.find_element(by=By.XPATH, value=f"/html/body/fsr-app/fsr-flights-search-result/fsr-qsf-layout/section/div/flights-list/div/div[2]/div/div/ul/li[{num}]/esky-flight-offer-group/div[1]/div[2]/div/leg-group/div/div/div/span[2]")
                        arrival_time_html = arrival_time.text
                        arrival_times.append(arrival_time_html)
                    except NoSuchElementException:
                        pass

                    try:
                        next_day_arrival = driver.find_element(by=By.XPATH, value=f"/html/body/fsr-app/fsr-flights-search-result/fsr-qsf-layout/section/div/flights-list/div/div[2]/div/div/ul/li[{num}]/esky-flight-offer-group/div[1]/div[2]/div/leg-group/div/div/div/span[3]")
                        next_day_arrival_html = next_day_arrival.text
                        if "+" in next_day_arrival_html:
                            next_day_arrivals.append(next_day_arrival_html)
                        else:
                            next_day_arrivals.append("")
                    except NoSuchElementException:
                        pass

                    try:
                        flight_time = driver.find_element(by=By.XPATH, value=f"/html/body/fsr-app/fsr-flights-search-result/fsr-qsf-layout/section/div/flights-list/div/div[2]/div/div/ul/li[{num}]/esky-flight-offer-group/div[1]/div[2]/div/leg-group/div/div/div/span[3]/span[2]")
                        flight_time_html = flight_time.text
                        flight_times.append(flight_time_html)
                    except NoSuchElementException:
                        try:
                            flight_time = driver.find_element(by=By.XPATH,
                                                                 value=f"/html/body/fsr-app/fsr-flights-search-result/fsr-qsf-layout/section/div/flights-list/div/div[2]/div/div/ul/li[{num}]/esky-flight-offer-group/div[1]/div[2]/div/leg-group/div/div/div/span[4]/span[2]")
                            flight_time_html = flight_time.text
                            flight_times.append(flight_time_html)
                        except NoSuchElementException:
                            pass

                last_seats = last_seats[:len(prices)]
                for price, flight_time, departure_time, arrival_time, next_day_arrival, last_seat, plane_change, airline in zip(prices, flight_times, departure_times, arrival_times, next_day_arrivals, last_seats, plane_changes, airlines):
                    table.put_item(Item={
                        'id':str(uuid.uuid4()),
                        'departureAirport': departure,
                        'arrivalAirport': arrival,
                        'date': date,
                        'prices': price,
                        'flightTimes': flight_time,
                        'departureTimes': departure_time,                        
                        'arrivalTimes': arrival_time,
                        'nexDayArrivals': next_day_arrival,
                        'lastSeats': last_seat,
                        'planeChanges': plane_change,
                        'airlines': airline,
                        'classType': class_type,
                        'timestamp': str(today)    
                        
                    })


    driver.close();
    driver.quit();

    response = {
        "statusCode": 200
    }

    return response