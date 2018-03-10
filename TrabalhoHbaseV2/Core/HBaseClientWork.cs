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
        static string host = "192.168.219.129";

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

                        var keys = rowResult.Columns.Select(c => Encoding.UTF8.GetString(c.Key));

                        var res = rowResult.Columns.Select(c => Encoding.UTF8.GetString(c.Value.Value));

                        string[] chave = new string[keys.Count()];
                        int i = 0;
                        foreach (var item in keys)
                        {
                            chave[i] = item.ToString();
                            i++;
                        }

                        int count = 0;
                        foreach (var cell in res)
                        {
                            switch (chave[count])
                            {
                                case "fc:ano":
                                    funcionario.Ano = Convert.ToInt32(cell);
                                    break;
                                case "fc:cpf":
                                    funcionario.Cpf = cell.ToString();
                                    break;
                                case "fc:jetons":
                                    funcionario.Jetons = cell.ToString();
                                    break;
                                case "fc:mes":
                                    funcionario.Mes = Convert.ToInt32(cell);
                                    break;
                                case "fc:nome":
                                    funcionario.Nome = cell.ToString();
                                    break;
                                case "fc:salario":
                                    funcionario.Salario = cell.ToString();
                                    break;                                                                   
                            }
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