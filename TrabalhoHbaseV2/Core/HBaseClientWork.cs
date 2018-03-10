using HBase.Thrift;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;
using Thrift.Protocol;
using Thrift.Transport;

namespace TrabalhoHbaseV2.Core
{
    public class HBaseClientWork
    {
        private static Hbase.Client _hbase;
        static byte[] table_name = Encoding.UTF8.GetBytes("pessoas");
        static readonly byte[] ID = Encoding.UTF8.GetBytes("idade");
        static readonly byte[] NAME = Encoding.UTF8.GetBytes("nome");
        static int i = 0;
        static int port = 9090;
        static string host = "192.168.139.128";

        public static void List()
        {
            try
            {
                var socket = new TSocket(host, port);
                var transport = new TBufferedTransport(socket);
                var proto = new TBinaryProtocol(transport);
                _hbase = new Hbase.Client(proto);
                transport.Open();

                //Conectado
                //var names = _hbase.getTableNames();
                //names.ForEach(msg => Console.WriteLine(Encoding.UTF8.GetString(msg)));

                transport.Close();
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static void Insert()
        {
            try
            {
                var socket = new TSocket(host, port);
                var transport = new TBufferedTransport(socket);
                var proto = new TBinaryProtocol(transport);
                _hbase = new Hbase.Client(proto);
                transport.Open();

                //Conectado

                transport.Close();
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static void Update()
        {
            try
            {
                var socket = new TSocket(host, port);
                var transport = new TBufferedTransport(socket);
                var proto = new TBinaryProtocol(transport);
                _hbase = new Hbase.Client(proto);
                transport.Open();

                //Conectado

                transport.Close();
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static void Search()
        {
            try
            {
                var socket = new TSocket(host, port);
                var transport = new TBufferedTransport(socket);
                var proto = new TBinaryProtocol(transport);
                _hbase = new Hbase.Client(proto);
                transport.Open();

                //Conectado

                transport.Close();
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}