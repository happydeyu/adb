package cn.yu.db.client;

import cn.yu.db.transport.Encoder;
import cn.yu.db.transport.Packager;
import cn.yu.db.transport.Transporter;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class Launcher {
    public static void main(String[] args) throws UnknownHostException, IOException {
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder e = new Encoder();
        Transporter t = new Transporter(socket);
        Packager packager = new Packager(t, e);

        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}