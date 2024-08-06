// This code clicks run in scrapy hub to start crawling. This needs to be run perodically because each scrapy_hub free runs last only an hour.

/*
# updating nodejs from v14 -> v16 :: reference : https://www.digitalocean.com/community/tutorials/how-to-install-node-js-on-debian-10
# !curl -fsSL https://deb.nodesource.com/setup_16.x | sudo -E bash -
# !sudo apt install nodejs
# !node -v

# !apt-get update && upgrade
## Install google chrome
!wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
# Install the package using the dpkg command:
!sudo dpkg -i google-chrome-stable_current_amd64.deb
# If you encounter any dependency errors during the installation, you can resolve them by running the following command:
!sudo apt-get install -f -y
# Verify the installation by running the following command:
!google-chrome --version


# !cp /usr/lib/chromium-browser/chromedriver /usr/bin
# import sys
# sys.path.insert(0,'/usr/lib/chromium-browser/chromedriver')
# !npm i convert-excel-to-json
!npm install puppeteer
!npm install puppeteer-extra
!npm install puppeteer-extra --save
!npm install puppeteer-extra-plugin-stealth --save


!wget https://storage.googleapis.com/chrome-for-testing-public/126.0.6478.182/linux64/chromedriver-linux64.zip

# Extract chromedriver
!unzip -o chromedriver-linux64.zip  # o flag to over-write

# Move chromedriver inside the applications folder
# !sudo mv chromedriver-linux64/chromedriver /usr/bin/chromedriver

# # Confirm chromedriver version
!chromedriver-linux64/chromedriver --version
*/


%%writefile soa_crawl.js
'use strict';


// stores code to file: 'soa_crawl.js'
// const excelToJson = require('convert-excel-to-json');
// const puppeteer = require('puppeteer');
const puppeteer = require('puppeteer-extra');

const StealthPlugin = require('puppeteer-extra-plugin-stealth')
puppeteer.use(StealthPlugin())

var fs = require('fs'); // to create folder if not exist // reference: https://colab.research.google.com/drive/168X6Zo0Yk2fzEJ7WDfY9Q_0UOEmHSrZc?usp=sharing

// Function to generate a random delay between min and max seconds
function getRandomDelay(min, max) {
  return Math.floor(Math.random() * (max - min + 1) + min) * 1000; // Convert to milliseconds
}

// to wait
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
// Generate a random delay between 5 to 10 seconds
let delay_ms = getRandomDelay(5, 10);


// if (!fs.existsSync('./drive/MyDrive')){
//   // credential for running locally
//   browser_executable_path = "/usr/bin/chromium-browser";
//   // browser_executable_path = "/usr/bin/chromedriver";
// }


async function myFunction(username, password, jobs_page, clone_index, run_index, initial_delay) {
  console.log('initial delay', initial_delay);
  await delay(initial_delay*1000);
  console.log(username);
  // ----------------------------------------------------------------------------
  // --------------------------------------  Crawl and save urls  ----------------
  // ----------------------------------------------------------------------------

    /*
    const browser = await puppeteer.launch({
      headless:false, ignoreHTTPSErrors: true,
      defaultViewport: null,
      // args: ['--start-fullscreen'],
      executablePath: browser_executable_path

    });*/



    // const browser = await puppeteer.launch({executablePath: '/opt/google/chrome/chrome', args:['--no-sandbox','--start-maximized'], ignoreHTTPSErrors: true, headless: true}); // aws
    // for google colab
    const browser = await puppeteer.launch({executablePath: "/opt/google/chrome/chrome", args:['--no-sandbox','--start-maximized']}); // colab
    // const browser = await puppeteer.launch({executablePath: '/usr/bin/chromium-browser', args:['--no-sandbox','--start-maximized']}); // colab

    const page = await browser.newPage();
    await page.setExtraHTTPHeaders({
        'Accept-Language': 'en-US,en;q=0.9',
      });
    console.log("visit the zyte")
    await page.goto('https://app.zyte.com/', {timeout: 0});
    //capture screenshot
    await page.screenshot({
        path: 'zyte_homepage.png'
    });


    // Accept cookies
    // Wait for the button to be available and then click it
      await page.waitForSelector('#onetrust-accept-btn-handler', { visible: true, timeout:0  });
      await page.click('#onetrust-accept-btn-handler');

      // Take screenshot
      await page.screenshot({
        path: 'cookies.png'
    });
    console.log('accepting cookies');

    // Enter username
    await page.type('#id_username', username);
    // Enter password
    await page.type('#id_password', password);

    // Take screenshot
    await page.screenshot({path: 'entered password.png'});

    // click sign in with email
    // Wait for the element with class 'mat-ripple' to be available and then click it
    await page.waitForSelector('.mat-ripple', { visible: true, timeout:0  });
    await page.evaluate(() => {
      document.getElementsByClassName('mat-ripple')[0].click();
    });



    console.log('signing in');
    let sign_in_delay = getRandomDelay(5, 10);
    console.log(`sign in delay:  ${sign_in_delay / 1000} seconds...`);
    await delay(sign_in_delay);
    console.log('..');
    // console.log(`Waiting for ${delay_ms / 1000} seconds after signing in...`);
    // Wait for the random delay
    // await delay(delay_ms);


    // Take screenshot
    await page.screenshot({
      path: 'signed in.png'
    });

    // sleep a while

    /*
    js code
    --------
    // click on scrapy project
    // last element
    document.getElementsByClassName('dashboard-service-card__item')[document.getElementsByClassName('dashboard-service-card__item').length-1].click()
    // click on latest version
    document.getElementsByClassName('checkbox-custom')[5].click()

    // clone project
    document.getElementsByClassName('btn-content')[7].click()

    // click run
    document.getElementsByClassName('mat-ripple control-element')[9].click()
    //browser close

    */
    // Click on the last element with class 'dashboard-service-card__item'
    // await page.waitForSelector('.dashboard-service-card__item', { visible: true, timeout:0  });
    // await page.evaluate(() => {
    //   const items = document.getElementsByClassName('dashboard-service-card__item');
    //   items[0].click();
    //   // items[items.length - 1].click();
    // });
    await page.goto(jobs_page, {timeout:0});



    // checkbox: select last project
    // Click on the 6th element with class 'checkbox-custom'
    await page.waitForSelector('.checkbox-custom', { visible: true, timeout:0 });
    // Take screenshot
    await page.screenshot({path: 'opened jobs page.png'});
    console.log('opened jobs page', jobs_page);

    // ------------------------------------------------------------------
    // it was trying to clone job. lets justtry to run a new job instead.
    await page.evaluate(() => {
      // select job
      document.getElementsByClassName('checkbox-custom')[5].click();
    });
    console.log('select project to clone.');

    await page.screenshot({path: 'checkbox.png'});

    // click clone
    // Click on the 7th element with class 'btn-content'
    await page.waitForSelector('.btn-content', { visible: true, timeout:0 });
    // let the_clone_index = config.clone_index;
    // let the_run_index = run_index;
    await page.evaluate((CloneIndex) => {
      document.getElementsByClassName('btn-content')[CloneIndex].click();
    }, clone_index);

    await page.screenshot({path: 'clone.png'});

    // await page.evaluate(() => {
    //   // select job
    //   document.getElementsByClassName('btn-content')[1].click();
    // });
    // ------------------------------------------------------------------

    // Click on the 9th element with class 'mat-ripple control-element'
    await page.waitForSelector('.mat-ripple.control-element', { visible: true, timeout:0  });
    await page.evaluate((RunIndex) => {
      document.getElementsByClassName('mat-ripple control-element')[RunIndex].click();
    }, run_index);


    await page.screenshot({path: 'clicked_run.png'});
    console.log('clicked_run.png');
    // sleep a while

    // Generate a random delay between 5 to 10 seconds
    delay_ms = getRandomDelay(5, 10);
    console.log(`Waiting for ${delay_ms / 1000} seconds before closing the browser...`);

    // Wait for the random delay
    await delay(delay_ms);

    await page.screenshot({path: 'browser_close.png'});
    await browser.close();
    console.log('..');

}


/*

// to get run index and clone index

// check the project
document.getElementsByClassName('checkbox-custom')[5].click()

// clone index
document.getElementsByClassName('btn-content')[6].click()

// run index
document.getElementsByClassName('mat-ripple control-element')[8].click()
*/
let configs = [
  {'username':'aanandaprashadgiri@yahoo.com', 'password':'zyte_password', 'jobs_page':'https://app.zyte.com/p/753056/jobs', 'clone_index':8, 'run_index':10}, // checked
  ...
]

// var delay_time=0
// for (let config of configs){
//     myFunction(config.username, config.password, config.jobs_page, config.clone_index, config.run_index, delay_time);

//     // 30 seconds delay before each next url
//     delay_time += 30000;  // +30S delay
//     delay(delay_time);
// }

var delay_time=0
async function processConfigs() {
  for (let config of configs) {
    try {
      await myFunction(config.username, config.password, config.jobs_page, config.clone_index, config.run_index, delay_time);
    } catch (error) {
      console.error(`Error processing config: ${config.username} - ${error.message}`);
    }
  }
  console.log('processed all configs..');
  process.exit(0); // Terminate the process with a successful exit code (0)
}

processConfigs();