using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using TrabalhoHbaseV2.Core;

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
            HBaseClientWork.List();
            return View();
        }

        public ActionResult Ranking()
        {
            return View();
        }
    }
}