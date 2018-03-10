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
        public ActionResult Index()
        {
            return View();
        }

        public ActionResult List()
        {
            var list = HBaseClientWork.List("");
            return View(list);
        }

        public ActionResult Ranking()
        {
            return View();
        }

        [HttpPost]
        public ActionResult Buscar(string filtro)
        {
            var list = HBaseClientWork.List(filtro);
            return View("List", list);
        }
    }
}