﻿using HBase.Thrift;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;
using Thrift.Protocol;
using Thrift.Transport;
using TrabalhoHbaseV2.Models;

namespace TrabalhoHbaseV2.Core
{
    public class HBaseClientWork
    {
        private static Hbase.Client _hbase;
        static byte[] table_name = Encoding.UTF8.GetBytes("pessoas");
        static readonly byte[] Family = Encoding.UTF8.GetBytes("idade");
        //static readonly byte[] NAME = Encoding.UTF8.GetBytes("nome");
        static int i = 0;
        static int port = 9090;
        static string host = "192.168.139.128";

        public static ListModel List()
        {
            try
            {
                var list = new ListModel();
                list.Funcionarios = new List<FuncionarioModel>();
                

                var socket = new TSocket(host, port);
                var transport = new TBufferedTransport(socket);
                var proto = new TBinaryProtocol(transport);
                _hbase = new Hbase.Client(proto);
                transport.Open();

                //Conectado
                var scanner = _hbase.scannerOpen(table_name, Guid.Empty.ToByteArray(), new List<byte[]>() { Family });
                for (var entry = _hbase.scannerGet(scanner); entry.Count > 0; entry = _hbase.scannerGet(scanner))
                {
                    foreach (var rowResult in entry)
                    {
                        var funcionario = new FuncionarioModel();
                        funcionario.Key = Encoding.UTF8.GetString(rowResult.Row);
                        var res = rowResult.Columns.Select(c => BitConverter.ToInt32(c.Value.Value, 0));
                        //foreach (var cell in res)
                        //{
                        //    Console.WriteLine("{0}", cell);
                        //}

                        list.Funcionarios.Add(funcionario);
                    }
                }

                transport.Close();

                return list;
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