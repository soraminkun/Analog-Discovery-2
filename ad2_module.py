from ctypes import *
import math,time,datetime,os,sys,csv,json
import boto3
import asyncio

S3_BUCKET_NAME = "" #S3のバケット名を指定
S3_FOLDER_ROUTE = "" #S3のフォルダルートを指定


class DWFAcquisition:
    def __init__(self,frequency,custom_filename=""):
        if sys.platform.startswith("win"):
            self.dwf = cdll.dwf
        elif sys.platform.startswith("darwin"):
            self.dwf = cdll.LoadLibrary("/Library/Frameworks/dwf.framework/dwf")
        else:
            self.dwf = cdll.LoadLibrary("libdwf.so")
        
        self.hdwf = c_int()
        self.sts = c_byte()
        self.secLog = 0.1  # logging rate in seconds
        self.frequency=frequency
        self.nSamples = 8000
        self.rgdSamples = (c_double * self.nSamples)()
        self.cValid = c_int(0)
        #create filename
        now = datetime.datetime.today()
        hour_str =  now.strftime("%H%M%S")
        frequency_str = f"{int(self.frequency)}Hz"
        self.filename = f"record_{now.strftime('%Y%m%d')}_{hour_str}_{frequency_str}_{custom_filename}.csv"


        
    def open_device(self):
        self.dwf.FDwfDeviceOpen(c_int(-1), byref(self.hdwf))
        if self.hdwf.value == 0:
            szerr = create_string_buffer(512)
            self.dwf.FDwfGetLastErrorMsg(szerr)
            print(str(szerr.value))
            print("failed to open device")
            quit()

    def configure_signal_acquisition(self):
        
        #AC source
        self.dwf.FDwfAnalogOutNodeEnableSet(self.hdwf, c_int(0), c_int(0), c_int(1))  # carrier
        self.dwf.FDwfAnalogOutNodeFunctionSet(self.hdwf, c_int(0), c_int(0), c_int(1))  # sine
        self.dwf.FDwfAnalogOutNodeFrequencySet(self.hdwf, c_int(0), c_int(0), c_double(self.frequency))  # Hz
        self.dwf.FDwfAnalogOutNodeAmplitudeSet(self.hdwf, c_int(0), c_int(0), c_double(5))  # Amplitude(V)
        self.dwf.FDwfAnalogOutNodeOffsetSet(self.hdwf, c_int(0), c_int(0), c_double(0))  # offset(0V)
        self.dwf.FDwfAnalogOutConfigure(self.hdwf, c_int(0), c_int(1))

        #Logger
        self.dwf.FDwfAnalogInChannelEnableSet(self.hdwf, c_int(0), c_int(1))
        self.dwf.FDwfAnalogInChannelRangeSet(self.hdwf, c_int(0), c_double(50))  # range of Volt
        self.dwf.FDwfAnalogInAcquisitionModeSet(self.hdwf, c_int(1))  # acqmodeScanShift
        self.dwf.FDwfAnalogInFrequencySet(self.hdwf, c_double(self.nSamples / self.secLog))
        self.dwf.FDwfAnalogInBufferSizeSet(self.hdwf, c_int(self.nSamples))
        time.sleep(1)

        # begin acquisition
        self.dwf.FDwfAnalogInConfigure(self.hdwf, c_int(0), c_int(1))
        
    async def acquire_and_log_data(self,folder_route,stop_event,websocket):
        try:
            os.makedirs(folder_route, exist_ok=True)
            log_file_path=os.path.join(folder_route,self.filename)
            await self.log_data(log_file_path,stop_event,websocket)
            #self.upload_to_s3(folder_route, self.filename)
        except KeyboardInterrupt:
            pass
        
    async def log_data(self, filename, stop_event,websocket):
        with open(filename, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "dc", "ACRMS[V]", "DCRMS[V]"])

            while not stop_event.is_set():
                await asyncio.sleep(self.secLog)
                self.dwf.FDwfAnalogInStatus(self.hdwf, c_int(1), byref(self.sts))
                self.dwf.FDwfAnalogInStatusSamplesValid(self.hdwf, byref(self.cValid))
                for iChannel in range(1):
                    self.dwf.FDwfAnalogInStatusData(self.hdwf, c_int(iChannel), byref(self.rgdSamples), self.cValid)
                    dc = 0
                    for i in range(self.nSamples):
                        dc += self.rgdSamples[i]
                    dc /= self.nSamples
                    dcrms = 0
                    acrms = 0
                    for i in range(self.nSamples):
                        dcrms += self.rgdSamples[i] ** 2
                        acrms += (self.rgdSamples[i] - dc) ** 2
                    dcrms /= self.nSamples
                    dcrms = math.sqrt(dcrms)
                    acrms /= self.nSamples
                    acrms = math.sqrt(acrms)

                    timestamp = datetime.datetime.today() #YY-MM-DD HH:MM:SS 
                    
                    print(f"CH:{iChannel + 1} time:{timestamp} dc:{dc:.7f}V ACRMS:{acrms:.7f}V DCRMS:{dcrms:.7f}V")
                    
                    data = [timestamp, dc, acrms, dcrms]
                    writer.writerow(data) #write csv
                    
                    data = {'dcrms':dcrms, 'acrms':acrms}
                    await websocket.send(json.dumps(data)) #transform to JSON and send to nodered
        f.close()


    def close_device(self):
        self.dwf.FDwfAnalogOutConfigure(self.hdwf, c_int(0), c_int(0))
        self.dwf.FDwfDeviceCloseAll()
        
    def upload_to_s3(self, folder_route, filename):
        with open(filename, "rb") as f:
            s3_client = boto3.client("s3")
            s3_client.upload_fileobj(f, S3_BUCKET_NAME, folder_route + filename)

