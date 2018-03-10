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
    public class HBaseClientWorkV2
    {
        private static Hbase.Client _hbase;
        static byte[] table_name = Encoding.UTF8.GetBytes("remuneracao");
        static readonly byte[] Family = Encoding.UTF8.GetBytes("fc");
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

        public static void Insert(FuncionarioModel model)
        {
            try
            {
                var socket = new TSocket(host, port);
                var transport = new TBufferedTransport(socket);
                var proto = new TBinaryProtocol(transport);
                _hbase = new Hbase.Client(proto);
                transport.Open();

        

                _hbase.mutateRows(table_name, new List<BatchMutation>()
                {
                    new BatchMutation()
                    {
                        Row = Encoding.UTF8.GetBytes(model.GetRowKey()),
                        Mutations = new List<Mutation> {
                            new Mutation{Column = Encoding.UTF8.GetBytes("fc:ano"), IsDelete = false, Value = Encoding.UTF8.GetBytes(model.Ano.ToString()) },
                            new Mutation{Column = Encoding.UTF8.GetBytes("fc:mes"), IsDelete = false, Value = Encoding.UTF8.GetBytes(model.Mes.ToString()) },
                            new Mutation{Column = Encoding.UTF8.GetBytes("fc:cpf"), IsDelete = false, Value = Encoding.UTF8.GetBytes(model.Cpf.ToString()) },
                            new Mutation{Column = Encoding.UTF8.GetBytes("fc:nome"), IsDelete = false, Value = Encoding.UTF8.GetBytes(model.Nome.ToString()) },
                            new Mutation{Column = Encoding.UTF8.GetBytes("fc:salario"), IsDelete = false, Value = Encoding.UTF8.GetBytes(model.Salario.ToString()) },
                            new Mutation{Column = Encoding.UTF8.GetBytes("fc:jetons"), IsDelete = false, Value = Encoding.UTF8.GetBytes(model.Jetons.ToString()) }

                        }
                    }
                });

                transport.Close();
            }
            catch (Exception)
            {
                throw;
            }
        }
        public static FuncionarioModel Get(string RowKey)
        {
            try
            {
                var socket = new TSocket(host, port);
                var transport = new TBufferedTransport(socket);
                var proto = new TBinaryProtocol(transport);
                _hbase = new Hbase.Client(proto);
                transport.Open();

                var funcionario = new FuncionarioModel();
                var scanner = _hbase.scannerOpen(table_name, Encoding.UTF8.GetBytes(RowKey), null);
                var entry = _hbase.scannerGet(scanner);
                foreach (var rowResult in entry)
                {
                     funcionario.Key = Encoding.UTF8.GetString(rowResult.Row);

          
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


                }


                transport.Close();
                return funcionario;
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