using HBase.Thrift;
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
        static byte[] table_name = Encoding.UTF8.GetBytes("remuneracao");
        static readonly byte[] Family = Encoding.UTF8.GetBytes("fc");
        //static readonly byte[] NAME = Encoding.UTF8.GetBytes("nome");
        static int i = 0;
        static int port = 9090;
        static string host = "192.168.248.129";

        public static ListModel List(string filtro)
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
                int scanner;
                scanner = _hbase.scannerOpen(table_name, Guid.Empty.ToByteArray(), new List<byte[]>() { Family });
                //if (string.IsNullOrEmpty(filtro))
                //    scanner = _hbase.scannerOpen(table_name, Guid.Empty.ToByteArray(), new List<byte[]>() { Family });
                //else
                //    scanner = _hbase.scannerOpenWithPrefix(table_name, Encoding.UTF8.GetBytes(filtro), new List<byte[]>() { Family });

                for (var entry = _hbase.scannerGet(scanner); entry.Count > 0; entry = _hbase.scannerGet(scanner))
                {
                    foreach (var rowResult in entry)
                    {
                        var funcionario = new FuncionarioModel();
                        funcionario.Key = Encoding.UTF8.GetString(rowResult.Row);

                        if (!funcionario.Key.ToUpper().Contains(filtro.ToUpper()))
                            continue;

                        var res = rowResult.Columns.Select(c => Encoding.UTF8.GetString(c.Value.Value));

                        int count = 0;
                        foreach (var cell in res)
                        {
                            if (count == 0)
                                funcionario.Ano = Convert.ToInt32(cell);
                            if (count == 1)
                                funcionario.Cpf = cell.ToString();
                            if (count == 2)
                                funcionario.Jetons = cell.ToString();
                            if (count == 3)
                                funcionario.Mes = Convert.ToInt32(cell);
                            if (count == 4)
                                funcionario.Nome = cell.ToString();
                            if (count == 5)
                                funcionario.Salario = cell.ToString();

                            count++;
                        }

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