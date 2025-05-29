/**
 * Main JavaScript file for Red Flags Profits
 * Basic functionality and navigation
 */

document.addEventListener("DOMContentLoaded", function () {
  // Mobile navigation toggle
  const navToggle = document.querySelector(".nav-toggle");
  const nav = document.querySelector(".nav");

  if (navToggle && nav) {
    navToggle.addEventListener("click", function () {
      nav.classList.toggle("active");
      navToggle.classList.toggle("active");
    });
  }

  // Smooth scrolling for navigation links
  const navLinks = document.querySelectorAll('.nav-link[href^="#"]');

  navLinks.forEach((link) => {
    link.addEventListener("click", function (e) {
      e.preventDefault();

      const targetId = this.getAttribute("href").substring(1);
      const targetElement = document.getElementById(targetId);

      if (targetElement) {
        targetElement.scrollIntoView({
          behavior: "smooth",
        });
      }

      // Close mobile nav if open
      if (nav) {
        nav.classList.remove("active");
      }
      if (navToggle) {
        navToggle.classList.remove("active");
      }
    });
  });

  // Add active class to navigation based on scroll position
  const sections = document.querySelectorAll("section[id]");
  const navLinksAll = document.querySelectorAll(".nav-link");

  window.addEventListener("scroll", function () {
    let current = "";

    sections.forEach((section) => {
      const sectionTop = section.offsetTop;
      const sectionHeight = section.clientHeight;

      if (window.pageYOffset >= sectionTop - 200) {
        current = section.getAttribute("id");
      }
    });

    navLinksAll.forEach((link) => {
      link.classList.remove("active");
      if (link.getAttribute("href") === `#${current}`) {
        link.classList.add("active");
      }
    });
  });

  console.log("📊 Red Flags Profits main.js loaded");
});
