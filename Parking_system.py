"""
@author: pavitra mehra
"""
#libraries 
import heapq        #used for storing slots in the form of heap
import psycopg2     #used for connecting to the postgredb
import logging      #used for logging error or execptions
import os           #used for file handling

#Configuration for logging error and exptions in the error file
logging.basicConfig(level=logging.DEBUG,filemode='w')
formatter=logging.Formatter('%(levelname)s:%(name)s:%(message)s')
logger=logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
file_handler= logging.FileHandler('error.log')
logger.addHandler(file_handler)

#main class for automated ticketing system
class parking_system():
    
    def __init__(self):
        self.cur=None
        self.input_list=[]
        self.con=None
        self.slot_list=[]
        self.output_list=[]

    def auth(self,db_hostname,db_username,db_password,db_database):     #function for connecting to the database
        try:
            self.con=psycopg2.connect(
                    host=db_hostname,
                    database=db_database,
                    user=db_username,
                    password=db_password
            )
            self.cur=self.con.cursor()
        except psycopg2.DatabaseError as e:
            logger.error(e)
        
    def read(self):                                                     #function for reading the input file
        dir_path=os.getcwd()
        dir_path=os.path.join(dir_path,'input.txt')
        input_file=open(dir_path)
        for line in input_file:
            val=line.split(' ')
            self.input_list.append(val)
        input_file.close()
        
    def ticket_generation(self):                                        #function for running the input queries
        for l in self.input_list:
            if l[0]=="Create_parking_lot":                              #creating new slots for parking system
                 n=int(l[1])
                 self.slot_list=[*range(1,n+1)]
                 self.output_list.append("Created parking of "+l[1].rstrip()+" slots")
                 try:
                    query="Drop TABLE  IF EXISTS public.user"
                    self.cur.execute(query)
                    query="CREATE TABLE public.user(registration_no character varying(15),slot_no bigint NOT NULL,age integer,PRIMARY KEY (slot_no));"
                    self.cur.execute(query)
                 except psycopg2.DatabaseError as e:
                     logger.error(e)         
            elif l[0]=="Park":                                          #generating new ticket and giving slot to the nearest of entrace
                if len(self.slot_list)==0:
                    self.output_list.append("Parking slots are full\n")
                else:
                    try:
                        slot_no=heapq.heappop(self.slot_list)
                        query="insert into public.user(registration_no,age,slot_no) values('"+l[1]+"'"+","+l[3].rstrip()+","+str(slot_no)+");"
                        self.cur.execute(query)
                        self.output_list.append("Car with vehicle registration number "+l[1]+" has been parked at slot number "+str(slot_no))
                    except psycopg2.DatabaseError as e:
                        logger.error(e)
            elif l[0]=="Slot_numbers_for_driver_of_age":                                    #returning slot_no for drivers of particular age
                query="Select slot_no from public.user where Age="+l[1].rstrip()+";"
                try:
                    self.cur.execute(query)
                    slotno_list=[]
                    rows=self.cur.fetchall()
                    for r in rows:
                        slotno_list.append(r[0])
                    if len(slotno_list)!=0:
                        self.output_list.append(slotno_list)
                except psycopg2.DatabaseError as e:
                     logger.error(e)
            elif l[0]=="Slot_number_for_car_with_number":                                   #returning slot no of particular registration no
                query="Select slot_no from public.user where Registration_no='"+l[1].rstrip()+"';"
                try:
                    self.cur.execute(query)
                    slotno_list=[]
                    rows=self.cur.fetchall()
                    for r in rows:
                        slotno_list.append(r[0])
                    self.output_list.append(slotno_list)
                except psycopg2.DatabaseError as e:
                     logger.error(e)
            elif l[0]=="Vehicle_registration_number_for_driver_of_age":                 #returning vehicle registration no. for driver of a particular age
                query="Select Registration_No from public.user where Age="+l[1].rstrip()+";"
                try:
                    self.cur.execute(query)
                    regno_list=[]
                    rows=self.cur.fetchall()
                    for r in rows:
                        regno_list.append(r[0])
                    self.output_list.append(regno_list)
                except psycopg2.DatabaseError as e:
                    logger.error(e)
            elif l[0]=="Leave":                                                         #deleting the record of the vehicle left from the parking slot
                if int(l[1]) in self.slot_list:
                    self.output_list.append("Parking slot is already emty\n")
                else:
                    heapq.heappush(self.slot_list,int(l[1]))
                    query1="select *from public.user where slot_no="+l[1].rstrip()+";"
                    try:
                        self.cur.execute(query1)
                        rows=self.cur.fetchall()
                        for r in rows:
                            self.output_list.append("Slot number 2 vacated, the car with vehicle registration number "+ r[0]+" left the space, the driver of the car was of age "+ str(r[2]))
                        query2="delete from public.user where slot_no="+l[1].rstrip()+";"
                        self.cur.execute(query2)
                    except psycopg2.DatabaseError as e:
                         logger.error(e)       
        self.con.commit()                                                   #commiting the changes to the database
        
    def write(self):                                                        #function for writing to the output file
        dir_path=os.getcwd()
        dir_path=os.path.join(dir_path,'output.txt')
        outputfile=open(dir_path,'w')
        for line in self.output_list:
            if isinstance(line, list): 
                str1 = ','.join([str(e) for e in line])
                outputfile.write(str1)
            else:
                outputfile.write(line)
            outputfile.write("\n")
        print("output file is successfully writen")
        outputfile.close()
        
 
    def service(self):                                                      #service function for running all the code for running the automated ticket system
        self.auth("localhost","postgres","password","Parking_lot")
        self.read()
        self.ticket_generation()
        self.write()
    
    
ticket_system=parking_system()                                          #object of main class
ticket_system.service()                                                 #running the service function of main class
