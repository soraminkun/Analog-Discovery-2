import asyncio
import websockets
import json
import os,datetime

import ad2_module

websocket_server = ""  # Node-RED WebSocketサーバーのアドレスを指定

is_measuring = False

async def connect_to_node_red_websocket():
    
    is_measuring = False
    stop_event = None

    
    
    async with websockets.connect(websocket_server) as websocket:
        
        while True:
            # メッセージを受信

            message = json.loads(await websocket.recv())
            print(f"Received message: {message}")            
            print(f"Received message: {message['status']}")            
            
            if message['status'] == "ON" and not is_measuring :
                dwf_acquisition = ad2_module.DWFAcquisition(message['frequency'],message['filename'])
                dwf_acquisition.open_device()
                dwf_acquisition.configure_signal_acquisition()
                #dwf_acquisition.acquire_and_log_data(os.path.join('record' + datetime.datetime.today().strftime("%Y%m%d")))
                is_measuring = True
                await websocket.send("measuring")
                stop_event = asyncio.Event()
                measurement_task = asyncio.create_task(dwf_acquisition.acquire_and_log_data(os.path.join('record' + datetime.datetime.today().strftime("%Y%m%d")),stop_event,websocket))
                
            elif message['status'] == "OFF":
                is_measuring = False
                stop_event.set()
                await measurement_task
                await websocket.send("measurement stopped")
                dwf_acquisition.close_device()
                print("計測終了")
            

if __name__ == "__main__":
    asyncio.run(connect_to_node_red_websocket())


