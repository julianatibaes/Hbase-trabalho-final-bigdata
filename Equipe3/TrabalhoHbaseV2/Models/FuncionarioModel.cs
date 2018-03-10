using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace TrabalhoHbaseV2.Models
{
    public class FuncionarioModel
    {
        public string Key { get; set; }
        public int Ano { get; set; }
        public int Mes { get; set; }
        public string Nome { get; set; }
        public string Salario { get; set; }
        public string Jetons { get; set; }
        public string Cpf { get; set; }

        public string GetRowKey()
        {
            return Ano + "_" + Mes + "_" + Cpf + "_" + Nome;
        }
        public string GetCPF()
        {
            if (Key != null)
                return Key.Split('_')[2];
            else
                return "";
        }
    }
}