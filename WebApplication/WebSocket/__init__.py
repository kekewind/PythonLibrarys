import json
import logging
import time

from Web_Socket_Server import WebSocketServer


def message_received(client, server, msg):
    """
    msg:{
        id:                  client unique identify
        to_id:               client direction want to send to (can be none)
        server_handler_type: call which server function to process the msg throw none exit exception (can be none)
        body:                the data main content
    }
    return:{
        from_id:             client unique identify
        body:                the data main content
    }
    """
    try:
        info = json.loads(msg)
        msg_body = str(info.get("body"))
        server_handler_type = info.get("server_handler_type")
        return_msg = str(msg_body)
        if server_handler_type is not None:
            callback = server.get_handler_fn(server_handler_type)
            if callback is not None:
                return_msg = callback(msg_body)
        if info.get("to_id") is not None:
            to_client = server.get_client_by_id(str(info.get("to_id")))
            if to_client is not None:
                server.send_message(to_client, json.dumps({"from_id": client["id"], "body": return_msg}))
        else:
            server.send_message(client, json.dumps({"from_id": client["id"], "body": return_msg}))
    except json.JSONDecodeError:
        server.send_message(client, "msg is not illegal")


def fun(client, server):
    with open("WebGisMapToWeb.log", 'r', encoding='utf-8') as f:
        alls=f.read().replace("\n", "").replace("\t","")
        infos = json.loads(alls)
        for each in infos:
            time.sleep(0.1)
            server.send_message(client, json.dumps(each))


socket_server = WebSocketServer(13254, host='127.0.0.1', loglevel=logging.INFO)
socket_server.set_fn_message_received(message_received)
socket_server.set_fn_new_client(fun)
socket_server.run_forever()
