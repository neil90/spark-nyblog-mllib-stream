import socket
import time
import csv
import sys

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("localhost",9999))
s.listen(1)
print("Started...")
while(1):
	c,address = s.accept()
	with open("NYTimesBlogTest.csv", 'r') as csvfile:
		reader = csv.reader(csvfile)
		next(reader)
		for row in reader:			

			combinedrow = '\001'.join(row) + '\n'
		
			sys.stdout.write('\r{0}'.format(combinedrow))
		#print(combinedrow, flush=True)
			sys.stdout.flush()
				
			byterow = combinedrow.encode()

			c.send(byterow)
		#c.send(row.encode(encoding='utf_8'))
			time.sleep(.1)
	#c.close()
	break
#time.sleep(0.5)
c.close()
