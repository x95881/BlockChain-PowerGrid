
void setup()
{
 // initialize serial communication:
    Serial.begin(9600);
  
    
     
    

  //SCAN forward
  // scan left
  // scan right 
  // if nothing to sides or forward 4 ft 
    // go forward 4 ft  
  // elseif somehting to sides  
    // while somthing to left/right
      // if  something to right
        // tilt left 
      // elseif something to the left 
        // tilt right
      // 
}



void loop()
{
  long lPh, rPh, lPi, rPi;
  lPh = leftPhoto();
  rPh = rightPhoto();
  lPi = leftPing();
  rPi = rightPing();
  // WHILE there are no obstacles,and you're in the hallway, go forward 
  if(lPh > 800 || rPh > 600){ //directly next to light source
    exit;
  }
  else if(lPh > 200 && rPh > 200){ //hallway
    if(lPi < 60){ 
      slightRight();
    }
    else if(rPi < 60){
      slightLeft();
    }
    else{
      goForward5in(); 
    }
  }
  else{ //dark room
   
    if(lPh > rPh){
      if(lPi < 30){
        slightRight();
      }
      else if(abs(lPh-rPh) < 20){
        goForward3in(); 
      }
      else{
        slightLeft();
      }
    }
    else{
      if(rPi < 30){
        slightLeft();
      }
      else if(abs(lPh-rPh) < 20){
        goForward3in(); 
      }
      else{
        slightRight();
      }
    }
  }
  
  
  delay(2);
}
