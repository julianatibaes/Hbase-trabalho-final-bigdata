using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using TrabalhoHbaseV2.Core;
using TrabalhoHbaseV2.Models;

namespace TrabalhoHbaseV2.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Index(string key)
        {
            var model = new FuncionarioModel();
            
            if (key != null)
            {
                model = HBaseClientWorkV2.Get(key);
            }

            return View(model);
        }
        [HttpPost]
        public ActionResult Index(FuncionarioModel model)
        {
            HBaseClientWorkV2.Insert(model);
            ViewBag.Message = "Registro Alterado";
            return View(model);
        }
        public ActionResult List()
        {
            var list = HBaseClientWork.List("");
            return View(list);
        }

        public ActionResult Ranking()
        {
            ListModel list = HBaseClientWork.List("");
            list.Funcionarios = list.Funcionarios.OrderByDescending(f => Convert.ToDecimal(f.Salario)).ToList();
            return View(list);
        }

        [HttpPost]
        public ActionResult Buscar(string filtro)
        {
            var list = HBaseClientWork.List(filtro);
            return View("List", list);
        }
    }
}