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
    }
}