%global imrate;       *cutoff for block imputation rate;
%global rno;          *random number start;
%global s2uniq;       *count of selected uniqs by key2;
%global state;
%global allhouse;     *count of households;
%global popsize;      *count of persons;
%global subject;      *number subjected to swapping(-imputes);
%global target;       *percentage of households to be swapped;
%global deselect;     *number of households to be dropped;
%global p1floor;      *maximum percentage of prep=1 dropped (odd);
%global p2floor;      *maximum percentage of prep=2 dropped;
%global p3floor;      *maximum percentage of prep=3 dropped;
%global tolrnc;       *margin for completion of deselect;

%global prob_cutoff1; *probability that an hhsize bin keeps its orig value when noised;
%global prob_cutoff2; *prob_cutoff2-prob_cutoff1 = probability that an hhsize bin decreases by 1 / 1-prob_cutff2. = probability that an hhsize bin increases by 1;

%global pprop1;       *probability of a record being in group 1;
                      * (pseudo_tract=tract);
%global pprop2;       *probability of a record being in group 2;
                      * (pseudo_tract chosen within county);


%include 'header.txt';         *parameter file;

/*************************************************
The target swapping rate determines the percent of records swapped.

The random number is still used in randomly de-selecting records 
for swapping, after initially selecting all records.

The p3floor determines the maximum percent of prep=3 records that 
can be de-selected. Because we set all records to prep=3, p3floor 
must be larger than 1-target swap rate. Practically, we can set 
p3floor as 0.99 and leave it there (note, I am not sure if the 
code will run correctly if you set any of the floor macros to 1).

The p1floor and p2floor variables are not relevant because we 
assign all records prep=3, meaning there are no prep=1 or prep=2
records subject to swapping. The imrate variable is not relevant 
because the part of the dynamic record selection based on 
imputation rate, among other things, is no longer used. * I propose deleting this comment once we have confirmed that we are confident it is correct
***************************************************/

libname library '.';
libname me '.';                               *everything set to directory
                                               from which the program is run;

**filename orig "&ifile";                    *input data;
filename edited "swap00&state..dat";       *output,list of pairs to be swapped;
filename summary "sum00&state..txt";       *record counts;
filename debug "dbug00&state..txt";        *samples of major files;
filename stat "&state.stat.dat";

/* options cc=print; */ 
options pagesize=300;
options fullstimer;
options linesize=72;

proc printto print=summary new;
run;                                       *init run summary;
data _null_;
   file print;
   put;
   put "the input data file is:  &ifile";
   put "the state identifier is: &state";
   put "the impute cutoff is:  &imrate";
   put "the random start is:  &rno";
   put "the target rate is:  &target";
   put "the floor for prep=1 is:  &p1floor";
   put "the floor for prep=2 is:  &p2floor";
   put "the floor for prep=3 is:  &p3floor";
   put "the proportion for hhsize noise grp 1 is: &prob_cutoff1";
   put "the proportion for hhsize noise grp 2 is: &prob_cutoff2 minus &prob_cutoff1";
   put "the proportion for pseudogeog grp 1 is:  &pprop1";
   put "the proportion for pseudogeog grp 2 is:  &pprop2";

   put;
run;
proc printto;
run;
proc printto print=debug new;
run;                                      *init debug summary;
proc printto;
run;

*macro for reporting dataset size (BRAAM);
%macro HOWMANY (LIB,DSN)  ;
  %global NUMOBS ;
  %let NUMOBS = 0 ;
  proc sql noprint ;
   select nobs-delobs into :NUMOBS
     from dictionary.tables
     where libname =%upcase("&LIB") and memname=%upcase("&DSN") ;
  quit ;
%mend HOWMANY ;

%put &statenum.;
proc contents data=me.swap_hcef;
run;

proc print data=me.swap_hcef(where=(input(tabblkst, best2.) eq 2));
run;

%macro readin;

/*********************************     
Load file created in preprocess.sas 
for a given state (the state is specified 
in header_newinput.txt))
**********************************/
data orig(keep=key);
set me.swap_hcef(where=(input(tabblkst, BEST2.) eq &statenum.));
county=put(tabblkcou,3.);
tract=put(tabtractce,6.);
block=put(tabblk,4.);
tenure=put(ten,1.);
key=0;
run;

proc sort data=orig out=sorig;           
                                          
  by descending key;
run;

data keylist;
  set sorig;
  by descending key;                      *get one of each and count them;
  retain kcount (0);
  if first.key then kcount=1;
  else kcount=kcount+1;
  if last.key then output;
run;
proc datasets nolist library=work;
    delete sorig orig;
run;

data keylist2(rename=(key=start));        *make formats;
  set keylist;
  label=_n_;                              *arbitrarily assign index number;
  fmtname="iky_&state";
  type="J";
run;
data keylist3(rename=(key=label));
  set keylist;
  start=_n_;
  fmtname="oky_&state";
run;
proc format library=library cntlin=keylist2;
run;
proc format library=library cntlin=keylist3;
run;

* read in all the data;

data orig2 (drop=ikey geo tenure)
     key2list (keep=key2)
     me.ckey_&state (keep=key geo);                     
   set me.swap_hcef(keep=control tabblkcou tabtractce tabblk ten household_size tabblkst where=(input(tabblkst, best2.) eq &statenum.));
	county=put(tabblkcou,3.);
	tract=put(tabtractce,6.);
	block=put(tabblk,4.);
	tenure=put(ten,1.);
	geo=cat(county,tract,block);
  ikey=0;
  key=input(ikey,8.);


	hhbin=household_size;

	hhbin_noised=hhbin;

  	call streaminit(1);
	rand1=rand("Uniform");
	if rand1>&prob_cutoff1. and rand1<=&prob_cutoff2. then hhbin_noised=hhbin_noised-1;
	if rand1>&prob_cutoff2. then hhbin_noised=hhbin_noised+1;
	chhbin_noised=put(hhbin_noised,2.);	

  key2=chhbin_noised;
run;

data orig2;
set orig2;
  call streaminit(1);
  u=rand("Uniform");
  if u<&pprop1 then pgroup=1;
  else if u<&pprop1+&pprop2 then pgroup=2;
  else pgroup=3;
run;

proc sort data=orig2;
 by county tract;
run;

data group1 group2 group3;
 set orig2;
  call streaminit(1);
 randsort=rand("Uniform");
 if pgroup=1 then output group1;
 if pgroup=2 then output group2;
 if pgroup=3 then output group3;
run;

data group1b;
 set group1;
 pseudo_county=county;
 pseudo_tract=tract;
run;

data group2a(keep=county tract randsort);
 set group2;
run;

data group3a(keep=county tract randsort);
 set group3;
run;

proc sort data=group2a;
 by county randsort;
run;

data group2b;
 merge group2 group2a(rename=(county=pseudo_county tract=pseudo_tract) drop=randsort);
run;

proc sort data=group3a;
 by randsort;
run;

data group3b;
 merge group3 group3a(rename=(county=pseudo_county tract=pseudo_tract) drop=randsort);
run;

data orig3;
 set group1b group2b group3b;
run;

proc printto print=debug;
run;

proc freq data=keylist;
  table kcount/out=klist;
  title 'class counts for long key (kcount=1 == uniques)';
run;
*how many uniques (kcount=1) keys with 2 records etc ;

options nodate;
proc print noobs data=orig3(obs=10);
   format key oky_&state..;
   title 'first 10 records in dataset, key should display in char';
run;
proc print noobs data=me.ckey_&state(obs=10);
   format key oky_&state..;
   title 'first 10 records in study dataset, key should display in char';
run;
*just a sample to see if it read in correctly and displays ok;
options linesize=72;

proc printto print=summary;
run;

data _null_ ;
  set klist(obs=1);
  file print;
  put '   ' count '  uniques';
  title;
run;

*need to analyze with respect to key2, this page faults badly, 
could be taken out if you want to skip the analysis;

proc sort data=key2list;
  by key2;
run;

data k2list;
  set key2list;
  by key2;                      *get one of each and count them;
  retain kcount (0);
  if first.key2 then kcount=1;
  else kcount=kcount+1;
  if last.key2 then output;
run;

proc printto print=debug;
run;

*how many uniques (kcount=1) keys with 2 records etc ;

proc freq data=k2list;
  table kcount/out=k3list;
  title 'class counts short key (kcount=1 == uniques)';
run;

proc printto;
run;

data _null_;
  file summary mod;
  if _n_=1 then put 'uniqueness distribution for short key';
  set k3list(obs=5);
  put kcount count;
  title;
run;

%HOWMANY(work,orig3);
data _null_;
  title;
  file summary mod;
  put;
  put "&NUMOBS  read in";
  %let allhouse = &NUMOBS;
run;
proc datasets nolist library=work;
    delete key2list keylist keylist2 keylist3;
run;
*end bookkeeping;


proc print data=orig2(keep=household_size hhbin rand1 hhbin_noised chhbin_noised);
run;

%mend readin;


%macro impute;

* put data in geographic order;

proc sort data=orig3 out=origsort;
  by pseudo_county pseudo_tract block;
run;

proc datasets nolist library=work;
delete orig3;
run;


data withrate;
set origsort end=last;  
length uniqkey tuniqr 3;
  uniqkey=0;
  tuniqr=0;

run;


proc datasets nolist library=work;
delete origsort rates2;
run;


%mend impute;

%macro unique;

proc sort data=withrate out=wrate1;
  by key2 pseudo_county pseudo_tract block;
run;

proc datasets nolist library=work;
delete withrate;
run;


data eligibl1(drop=rate)
     dimpute (drop=prep key rate);

  set wrate1;
  by key2 pseudo_county pseudo_tract block;

  length prep unique 3;
  if first.block and last.block then unique=1;
  else unique=0;

  ranno=ranuni(&rno);   
  *SUBJECTS ALL RECORDS TO SWAPPING RATHER THAN MOST SENSITIVE; 
  prep=3;
  output eligibl1;

run;

*some bookkeeping;

proc datasets nolist library=work;
delete wrate1;
run;

%HOWMANY(work,eligibl1);
data _null_;
  file summary mod;
  title;                          
  put;
  put "&NUMOBS  records subjected to swapping procedure";
  %let subject = &NUMOBS;
run;

* sort the data (backwards) in preparation for partnering, ranno does double
duty by making sure there is no ambiguity in the order;

proc sort data=eligibl1 out=eligibl2;
  by descending key2 descending pseudo_county descending pseudo_tract descending prep
     descending ranno;
run;

proc datasets nolist library=work;
    delete eligibl1;
run;

%mend unique;


%macro estswap;

*take a look at the set of records to be swapped, number swap sequences, 
count their length.;

data aseq1(keep=select uflag prep seqno)
     preswap;
     set eligibl2 end=last;
       by descending key2 descending pseudo_county descending pseudo_tract descending prep;

     length uflag select inkey sinkey seqno 4;
     retain select inkey sinkey;
     retain uflag seqno hseqno 0;

     if first.key2 then do;         
        if select>0 then do; 
          output aseq1;
        end;
        inkey=1;
        select=0;
        sinkey=0;
     end;
     else do;
        inkey=inkey+1;
     end;

     if prep>0 then do;
        select=select+1;
        sinkey=sinkey+1;
     end;
     else do;
        if select>0 then do; 
          output aseq1;
          select=0;
        end;
     end;
     if last then do;
	output aseq1;
     end;
     if select=1 then hseqno=hseqno+1;
     if select>0 then do;
        seqno=hseqno;
        output preswap;
     end;                                   *uflag counts unswappables;
     else seqno=0;
     if last then call symput('s2uniq',put(uflag,10.));
run;
proc sort data=preswap;        *need length of sequence the record belongs to;
  by seqno;                    *and parity;
run;
data aseq1a;
  set aseq1;
  if (mod(select,2)=0) then odd=0;
  else odd=1;
run;
proc sort data=aseq1a;
  by seqno;
run;
proc freq noprint data=aseq1a;
  table select/out=aseq2;
run;

*estimate the number of swaps currently marked,
 bring in the target number, put the difference into
 the global variable deselect;

data aseq3;
  set aseq2 end=last;
  retain csum 0;
  if select=1 then count=count-&s2uniq;
  if (mod(select,2)=0) then contrib=select*count;
  else contrib=(select+1)*count;
  csum=csum+contrib;
  if last then do;
     toswap=&allhouse*&target;
     dselect=csum-&allhouse*&target;
     output;  
     if dselect>0 then call symput('deselect',put(dselect,10.));
     else call symput('deselect',0);
  end;
run;
%HOWMANY(work,preswap);
data _null_;
  set aseq3;
  prop=(&NUMOBS-&s2uniq)/csum;
  title;
  file summary mod;
  put 'The number of households targeted:  ' toswap ' projected:  ' csum;
  put 'Proportion of proj swap which are select:     ' prop;
run;
%mend estswap;

*the next macro, reduce1, isolates the least sensitive population and determines
how much of it should be deselected.  a core subpopulation must be left
regardless.  the size of the core is determined in part by ,p1floor,.  the
amount to be dropped is calculated, tkodd+tkeven, and selection is made
binomially at the sequence level for that fixed size.  EG if there are 5 prep=1
selected households in the current sequence and we need to select 25 of the
remaining 100 prep=1, then we will deselect bin(5,1/4) of the 5.  The number to
select and the number remaining are incremented down as usual for fixed sample
selection.  The procedure is adapted for odd/even situations: where the number
taken is profitably rounded down, do so and increase the probablity for 
selection in the remainder.  nb if you push off too many, the sample size isnt 
reached.; 

%macro reduce1;
%let rno3 = 1;          *random number start;

*redefine prep, merge in sequence level data;
data preswap2;
  merge preswap(keep=seqno prep control key2 county tract pseudo_county pseudo_tract prep
                     ranno)
        aseq1a(keep=seqno select odd);
  by seqno;
  length oprep 3;
  oprep=prep;
  rn3=ranuni(&rno3);
run;

*bookkeeping;
proc freq noprint data=preswap2;
  table prep*oprep/out=redef;
run;
proc printto print=debug;
run;
proc printto;
run;
proc printto print=summary;
run;
proc print noobs data=redef(where=
           ((prep=2 and oprep=1) or(prep=3 and oprep in (1,2))));
   var prep oprep count;
   title 'redefine prep';
run;  
proc freq data=preswap2;
  table prep*odd/nopercent norow nocol out=initps;
  title 'initial data on selected (redefined)';
run;
proc printto;
run;

*only want the prep=1 population, sort includes rn3 to get random order;

proc sort data=preswap2(where=(prep=1)) out=round1;
  by seqno rn3;
run;

*need additional sequence level data-how many of this type;
proc freq noprint data=round1;
  table seqno/out=p1inseq;
run;
proc sort data=p1inseq;
  by seqno;
run;
data idseq;
  merge p1inseq(in=in1) round1(keep=seqno select);
  by seqno;
  if in1;
  if first.seqno;
run;

*make an estimate of the contribution from prep=1, this number goes into 
 fixed sample selection. count is the frequency of prep=1 gotten above;
data idseq2(keep=even1 odd1); 
  set idseq end=last;
  retain even1 odd1 0;
  if (mod(select,2)=0) then even1=even1+count;
  else odd1=odd1+count+1;
  if last then output;
run;

*dselp1 is the residual file, ds1 will go into the drop set, dlist
 this is the binomial fixed sample selection;

data dselp1(drop=even1 flag odd odd1 p1 rno2 tktotal 
                 p2 p1a tkodd tkeven take)
     ds1(keep=control odd key2 county tract pseudo_county pseudo_tract prep oprep ranno);
  retain flag odd1 even1 tkodd tkeven;
  if _n_=1 then do;
     set idseq2;
     tktotal=floor(&p1floor*odd1)+floor(&p1floor*even1);
     if &deselect/tktotal<1 then adjustp=&deselect/tktotal;
     else adjustp=1;
     tkodd=floor(&p1floor*odd1*adjustp);
     tkeven=floor(&p1floor*even1*adjustp);

                            *tkodd tkeven are the target sample sizes;
  end;
  merge round1 p1inseq(keep=count seqno rename=(count=p1));
  by seqno;
  rno2 = 1;          *random number start;
  if first.seqno then do;
    if odd=1 then do;
      if odd1<1 then do;   *this is to ensure a good 
                            p value for ranbin, SAS doesnt 
                            like p=1 (or 0);
         tkodd=0;
         odd1=1;
      end;
      if tkodd/odd1>1 then tkodd=odd1;
      if tkodd<0 then tkodd=0;
      if tkeven<0 then tkeven=0;
      p2=tkodd/odd1;
      if p2=1 or p2=0 then do;
         if p2=1 then take=p1;
         if p2=0 then take=0;
      end;                                 *finally, ranbin;
      else take=ranbin(rno2,p1,p2);
      if (mod(take,2)=0 and take>0) then take=take-1;  *push off!;
      if take>0 then do;
         output ds1;                       
         flag=take-1;
         tkodd=tkodd-take-1;               *bookkeeping for sample selection;
      end;
      else output dselp1;
      odd1=odd1-p1-1;
    end;                                 *slightly different strategy for the
                                          even side: divide by 2 multiply by 2;
    else do;                             
      p1a=floor(p1/2);                   *more push off occuring;
      if even1<1 then do;                
         tkeven=0;
         even1=1;                        *still have the pesky ranbin problem;
      end;
      if tkeven/even1>1 then tkeven=even1;
      if tkeven<0 then tkeven=0;
      p2=tkeven/even1;
      if p1a>0 then do;
         if p2=1 or p2=0 then do;
            if p2=1 then take=2*p1a;
            if p2=0 then take=0;
         end;                                    *finally;
         else take=2*(ranbin(rno2,p1a,p2));
      end;
      else take=0;
      if take>0 then do;
         output ds1;
         flag=take-1;                            *bookkeep;
         tkeven=tkeven-take;
      end;
      else output dselp1;
      even1=even1-p1;
    end;
  end;
  else do;
    if flag>0 then do;
       output ds1;
       flag=flag-1;
    end;
    else output dselp1;   
  end;  
  if last.seqno then flag=0;
run;

*evaluate how it went so the main program can determine if it needs
 reduce2;

*reassemble selected;
proc datasets nolist lib=work;
  delete preswap3;
run;
proc append base=preswap3 data=dselp1(drop=adjustp);
run;
proc append base=preswap3 data=preswap2(where=(prep ne 1) drop=odd);
run;
proc sort data=preswap3;
by seqno;
run;
                                             *evaluate sequences;
proc freq noprint data=preswap3;
  table seqno/out=r2select;
run;
data r2seq1a;
  set r2select(rename=(count=select));
  if (mod(select,2)=0) then odd=0;
  else odd=1;
run;
proc sort data=r2seq1a;
  by seqno;
run;                                         *random number start;
%let rno3 = 1;          
                                             *merge on seq level data;
data preswap4;
  merge preswap3(drop=select) r2seq1a(keep=seqno select odd);
  by seqno;
  rn3=ranuni(&rno3);
run;
proc printto print=debug;
run;
proc freq noprint data=preswap4;
  table prep*odd/nopercent norow nocol out=ps4;
  title 'after 1st phase';
run;
proc printto;
run;
data _null_;                              *post the results;
  title;
  retain sum4 sum0 0;
  set ps4(rename=(count=c4) where=(prep=1) keep=count prep);
  set initps(rename=(count=c0) where=(prep=1) keep=count prep) end=last;
  sum4=sum4+c4;
  sum0=sum0+c0;
  if last then do;
     r1=sum4/sum0;
     file summary mod;
     put 'fraction of prep=1 remaining: ' r1;
  end;  
run;
				*summarize then calculate how much to go;
proc freq noprint data=r2seq1a;
  table select/out=r2seq2;
run;
data r2seq3;
  set r2seq2 end=last;
  retain csum 0;
  if select=1 then count=count-&s2uniq;
  if (mod(select,2)=0) then contrib=select*count;
  else contrib=(select+1)*count;
  csum=csum+contrib;
  if last then do;
     toswap=&allhouse*&target;
     dselect2=csum-&allhouse*&target;
     output;
     if dselect2>0 then call symput('deselect',put(dselect2,10.));
     else call symput('deselect',0);
  end;
run;                                             *some more evaluation;
%HOWMANY(work,preswap4);
data _null_;
  set r2seq3;
  prop=(&NUMOBS-&s2uniq)/csum;
  title;
  file debug mod;
  put 'Proportion of proj swap which are select (round1):     ' prop;
run;
proc printto print=debug;
run;
proc print data=r2seq3;
  var toswap csum dselect2;
  title 'after prep=1 deselection';
run;
proc printto;
run;
proc datasets nolist lib=work;
  delete preswap2 preswap3 dlist;
run;                                   *this is the output to the main routine;
proc append base=dlist data=ds1(rename=(prep=nprep));
run;
%mend reduce1;

*just like reduce1;
%macro reduce2;
proc sort data=preswap4(where=(prep=2)) out=round2;
  by seqno rn3;
run;
proc freq noprint data=round2;
  table seqno/out=p2inseq;
run;
proc sort data=p2inseq;
  by seqno;
run;
data idseq3;
  merge p2inseq(in=in1) round2(keep=seqno select);
  by seqno;
  if in1;
  if first.seqno;
run;

*make an estimate of the maximum number of drops from
 prep=2;

data idseq4(keep=even1 odd1);
  set idseq3 end=last;
  retain even1 odd1 0;
  if (mod(select,2)=0) then even1=even1+count;
  else odd1=odd1+count+1;
  if last then output;
run;
data dselp2(drop=even1 flag odd odd1 p1 rno2 tkodd tkeven tktotal
                 take p2 p1a)
     ds2(keep=control odd key2 county tract pseudo_county pseudo_tract prep oprep ranno);
  retain flag tkodd tkeven odd1 even1 0;
  if _n_=1 then do;
     set idseq4;
     tktotal=floor(&p2floor*odd1+&p2floor*even1);
     if &deselect/tktotal<1 then adjustp=&deselect/tktotal;
     else adjustp=1;
     tkodd=floor(&p2floor*odd1*adjustp);
     tkeven=floor(&p2floor*even1*adjustp);
  end;
  merge round2 p2inseq(keep=count seqno rename=(count=p1));
  by seqno;

  rno2 = 1;          *random number start;
  if first.seqno then do;
    if odd=1 then do;
      if odd1<1 then do;   *this is to ensure a good 
                            p value for ranbin, SAS doesnt 
                            like p=1 (or 0). SAS macro also 
                            reads quotes inside of comments;
         tkodd=0;
         odd1=1;
      end;
      if tkodd/odd1>1 then tkodd=odd1;
      if tkodd<0 then tkodd=0;
      p2=tkodd/odd1;
      if p2=1 or p2=0 then do;
         if p2=1 then take=p1;
         if p2=0 then take=0;
      end;
      else take=ranbin(rno2,p1,p2);
      if (mod(take,2)=0 and take>0) then take=take-1;
      if take>0 then do;
         output ds2;
         flag=take-1;
         tkodd=tkodd-take-1;
      end;
      else output dselp2;
      odd1=odd1-p1-1;
    end;
    else do;
      p1a=floor(p1/2);
      if even1<1 then do;
         tkeven=0;
         even1=1;
      end;
      if tkeven/even1>1 then tkeven=even1;
      p2=tkeven/even1;
      if p1a>0 then do;
         if p2=1 or p2=0 then do;
            if p2=1 then take=2*p1a;
            if p2=0 then take=0;
         end;
         else take=2*(ranbin(rno2,p1a,p2));
      end;
      else take=0;
      if take>0 then do;
         output ds2;
         flag=take-1;
         tkeven=tkeven-take;
      end;
      else output dselp2;
      even1=even1-p1;
    end;
  end;
  else do;
    if flag>0 then do;
       output ds2;
       flag=flag-1;
    end;
    else output dselp2;   
  end;  
  if last.seqno then flag=0;
run;

proc datasets nolist lib=work;
  delete preswap5;
run;
proc append base=preswap5 data=dselp2(drop=adjustp);
run;
proc append base=preswap5 data=preswap4(where=(prep ne 2) drop=odd);
run;
proc sort data=preswap5;
by seqno;
run;
proc freq noprint data=preswap5;
  table seqno/out=r3select;
run;
data r3seq1a;
  set r3select(rename=(count=select));
  if (mod(select,2)=0) then odd=0;
  else odd=1;
run;
proc sort data=r3seq1a;
  by seqno;
run;
data preswap6;
  merge preswap5(drop=select) r3seq1a(keep=seqno select odd);
  by seqno;
run;
proc printto print=debug;
run;
proc freq data=preswap6;
  table prep*odd/nopercent norow nocol out=ps6;
  title 'after phase 2';
run;
proc printto;
run;
data _null_;
  retain sum6 sum0 0;
  set ps6(rename=(count=c6) where=(prep=2) keep=count prep);
  set initps(rename=(count=c0) where=(prep=2) keep=count prep) end=last;
  sum6=sum6+c6;
  sum0=sum0+c0;
  if last then do;
     r1=sum6/sum0;
     file summary mod;
     title;
     put 'fraction of prep=2 remaining: ' r1;
  end;  
run;
proc freq noprint data=r3seq1a;
  table select/out=r3seq2;
run;
data r3seq3;
  set r3seq2 end=last;
  retain csum 0;
  if select=1 then count=count-&s2uniq;
  if (mod(select,2)=0) then contrib=select*count;
  else contrib=(select+1)*count;
  csum=csum+contrib;
  if last then do;
     dselect3=csum-&allhouse*&target;
     output;
     if dselect3>0 then call symput('deselect',put(dselect3,10.));
     else call symput('deselect',0);
  end;
run;
%HOWMANY(work,preswap6);
data _null_;
  set r3seq3;
  prop=(&NUMOBS-&s2uniq)/csum;
  title;
  file debug mod;
  put 'Proportion of proj swap which are select (round2):     ' prop;
run;
proc printto print=debug;
run;
proc print data=r3seq3;
  var csum dselect3;
  title 'after prep=2 deselection';
run;
proc printto;
run;
proc append base=dlist data=ds2(rename=(prep=nprep));
run;
proc datasets nolist lib=work;
  delete ds1 ds2 preswap4;
run;
%mend reduce2;

*just like reduce1;
%macro reduce3;
proc sort data=preswap6(where=(prep=3)) out=round3;
  by seqno rn3;
run;
proc freq noprint data=round3;
  table seqno/out=p3inseq;
run;
proc sort data=p3inseq;
  by seqno;
run;
data idseq5;
  merge p3inseq(in=in1) round3(keep=seqno select);
  by seqno;
  if in1;
  if first.seqno;
run;

data idseq6(keep=even1 odd1);
  set idseq5 end=last;
  retain even1 odd1 0;
  if (mod(select,2)=0) then even1=even1+count;
  else odd1=odd1+count+1;
  if last then output;
run;
proc printto print=debug;
run;
proc print data=idseq6;
  title 'prep=3 by odd/even inflated for odd partners';
run;
proc printto;
run;

data dselp3(drop=even1 flag odd odd1 p1 rno2 tkodd tkeven tktotal
                 take p2 p1a)
     ds3(keep=control odd key2 county tract pseudo_county pseudo_tract prep oprep ranno);
  retain flag tkodd tkeven odd1 even1 0;
  if _n_=1 then do;
     set idseq6;
     tktotal=floor(&p3floor*odd1+&p3floor*even1);
     if &deselect/tktotal<1 then adjustp=&deselect/tktotal;
     else adjustp=1;
     tkodd=floor(&p3floor*odd1*adjustp);
     tkeven=floor(&p3floor*even1*adjustp);
  end;
  merge round3 p3inseq(keep=count seqno rename=(count=p1));
  by seqno;

  rno2= 1;          *random number start;
  if first.seqno then do;
    if odd=1 then do;
      if odd1<1 then do;   *this is to ensure a good 
                            p value for ranbin, SAS doesnt 
                            like p=1 (or 0);
         tkodd=0;
         odd1=1;
      end;
      if tkodd/odd1>1 then tkodd=odd1;
      if tkodd<0 then tkodd=0;
      p2=tkodd/odd1;
      if p2=1 or p2=0 then do;
         if p2=1 then take=p1;
         if p2=0 then take=0;
      end;
      else take=ranbin(rno2,p1,p2);
      if (mod(take,2)=0 and take>0) then take=take-1;
      if take>0 then do;
         output ds3;
         flag=take-1;
         tkodd=tkodd-take-1;
      end;
      else output dselp3;
      odd1=odd1-p1-1;
    end;
    else do;
      p1a=floor(p1/2);
      if even1<1 then do;
         tkeven=0;
         even1=1;
      end;
      if tkeven/even1>1 then tkeven=even1;
      if tkeven<0 then tkeven=0;
      p2=tkeven/even1;
      if p1a>0 then do;
         if p2=1 or p2=0 then do;
            if p2=1 then take=2*p1a;
            if p2=0 then take=0;
         end;
         else take=2*(ranbin(rno2,p1a,p2));
      end;
      else take=0;
      if take>0 then do;
         output ds3;
         flag=take-1;
         tkeven=tkeven-take;
      end;
      else output dselp3;
      even1=even1-p1;
    end;
  end;
  else do;
    if flag>0 then do;
       output ds3;
       flag=flag-1;
    end;
    else output dselp3;   
  end;  
  if last.seqno then flag=0;
run;

proc datasets nolist lib=work;
  delete preswap7;
run;
proc append base=preswap7 data=dselp3(drop=adjustp);
run;
proc append base=preswap7 data=preswap5(where=(prep ne 3));
run;
proc sort data=preswap7;
by seqno;
run;
proc freq noprint data=preswap7;
  table seqno/out=r4select;
run;
data r4seq1a;
  set r4select(rename=(count=select));
  if (mod(select,2)=0) then odd=0;
  else odd=1;
run;
proc sort data=r4seq1a;
  by seqno;
run;
data preswap8;
  merge preswap7(drop=select) r4seq1a(keep=seqno select odd);
  by seqno;
run;
proc printto print=debug;
run;
proc freq data=preswap8;
  table prep*odd/nopercent norow nocol out=ps8;
  title 'after phase 3';
run;
proc printto;
run;
data _null_;
  retain sum8 sum0 0;
  set ps8(rename=(count=c8) where=(prep=3) keep=count prep);
  set initps(rename=(count=c0) where=(prep=3) keep=count prep) end=last;
  sum8=sum8+c8;
  sum0=sum0+c0;
  if last then do;
     r1=sum8/sum0;
     file summary mod;
     title;
     put 'fraction of prep=3 remaining: ' r1;
  end;  
run;
proc freq noprint data=r4seq1a;
  table select/out=r4seq2;
run;
data r4seq3;
  set r4seq2 end=last;
  retain csum 0;
  if select=1 then count=count-&s2uniq;
  if (mod(select,2)=0) then contrib=select*count;
  else contrib=(select+1)*count;
  csum=csum+contrib;
  if last then do;
     dselect4=csum-&allhouse*&target;
     output;
     if dselect4>0 then call symput('deselect',put(dselect4,10.));
     else call symput('deselect',0);
  end;
run;
%HOWMANY(work,preswap8);
data _null_;
  set r4seq3;
  prop=(&NUMOBS-&s2uniq)/csum;
  title;
  file debug mod;
  put 'Proportion of proj swap which are select (round3):     ' prop;
run;
proc printto print=debug;
run;
proc print data=r4seq3;
  var csum dselect4;
  title 'after prep=3 deselection';
run;

proc printto;
run;
proc append base=dlist data=ds3(rename=(prep=nprep));
run;
proc datasets nolist lib=work;
  delete ds3 preswap5 preswap6 preswap7 preswap8;
run;
%mend reduce3;

*need to take the reduction list and set prep=0 for those records,
 picking up the main thread with eligibl2;
%macro deselect;
proc sort data=dlist(rename=(oprep=prep) drop=odd nprep);
  by descending key2 descending pseudo_county descending pseudo_tract descending prep
     descending ranno;
run;
data eligibl3(drop=check);
  merge eligibl2 dlist(in=in1 rename=(control=check));
  by descending key2 descending pseudo_county descending pseudo_tract descending prep
     descending ranno;
  if in1 then do;
       if control ne check then do;
          file summary mod;
          put 'MISMATCH ON DESELECT CONTROL NUMBERS';
       end;
       prep=0;
  end;
  if prep>0 then prep=1;       *dont want value of prep to effect the next sort;
run;
proc datasets nolist library=work;
    delete eligibl2;
run;

*re-mark selected and count the number selected in sequence. 0 indicates
not selected. when order is reversed 1 indicates current record
selected, subsequent record not selected. establish a countdown of records in
key and selected records in key;

proc sort data=eligibl3 out=eligibl4;
  by descending key2 descending pseudo_county descending pseudo_tract descending prep
     descending ranno;
run;
proc datasets nolist library=work;
    delete eligibl3;
run;
data select1;                     *this is like ,data preswap, in estswap;
     set eligibl4 end=last;
       by descending key2 descending pseudo_county descending pseudo_tract descending prep;

     length select inkey sinkey 4;
     retain select inkey sinkey;

     if first.key2 then do;         
        inkey=1;
        select=0;
        sinkey=0;
     end;
     else do;
        inkey=inkey+1;
     end;
     if prep>0 then do;
        select=select+1;
        sinkey=sinkey+1;
     end;
     else select=0;
     output select1;  
run;
proc datasets nolist library=work;
    delete eligibl4;
run;
proc sort data=select1 out=select2;
  by key2 pseudo_county pseudo_tract prep ranno;
run;
proc datasets nolist library=work;
    delete select1;
run;
proc printto print=debug;
run;
proc print data=select2(obs=200);
    var key2 tract uniqkey tuniqr unique prep select inkey sinkey
        key control county block ranno pseudo_county pseudo_tract;
    title 'first 200 records after selection, routine #1';
run;
proc printto;
run;

%mend deselect;

*patch in case you dont actually have any deselection (no merge);
%macro dselect2;

data select1;
     set eligibl2 end=last;
       by descending key2 descending pseudo_county descending pseudo_tract descending prep;

     length select inkey sinkey 4;
     retain select inkey sinkey;

     if first.key2 then do;         
        inkey=1;
        select=0;
        sinkey=0;
     end;
     else do;
        inkey=inkey+1;
     end;
     if prep>0 then do;
        select=select+1;
        sinkey=sinkey+1;
     end;
     else select=0;
     output select1;  
run;
proc datasets nolist library=work;
    delete eligibl2;
run;
proc sort data=select1 out=select2;
  by key2 pseudo_county pseudo_tract prep ranno;
run;
proc datasets nolist library=work;
    delete select1;
run;
proc printto print=debug;
run;
proc print data=select2(obs=200);
    var key2 tract uniqkey tuniqr unique prep select inkey sinkey
        key control county block ranno pseudo_tract pseudo_county;
    title 'first 200 records after selection, routine #2';
run;
proc printto;
run;

%mend dselect2;

%macro countgeo;       *called in data step [swapped];
           if county=scounty then wincty=wincty+1;
           else osdcty=osdcty+1;
           if (tract=stract and county=scounty) then wintrt=wintrt+1;
           else osdtrt=osdtrt+1;
           if uniqkey=1 then do;
                if county=scounty then nuwincty=nuwincty+1;
                else nuosdcty=nuosdcty+1;
                if (tract=stract and county=scounty) then nuwintrt=nuwintrt+1;
                else nuosdtrt=nuosdtrt+1;
           end;
%mend countgeo;

%macro georeprt;       *called in data step [swapped];
        title;
        file debug mod;
        put 'within tract:                 ' wintrt;
        put 'outside tract:                ' osdtrt;
        put 'within county:                ' wincty;
        put 'outside county:               ' osdcty;
        put ' ';
        put 'unswapped selected:           ' nopair;
        put 'unswapped selected key uniq: ' nopairu;
        put 'unique key:                  ' urcount;
        put 'swapped unique key           ' urswap;
        put 'the next 4 are by initiating record';
        put 'key uniq within tract:       ' nuwintrt;
        put 'key uniq outside tract:      ' nuosdtrt;
        put 'key uniq within county:      ' nuwincty;
        put 'key uniq outside county:     ' nuosdcty;
        put ' '; put ' ';
        pruu=100*nopairu/urcount;
        pahs=100*2*(wintrt+osdtrt)/left(&allhouse);
        puu=100*nopair/left(&subject);
        pswt=100*wintrt/(wintrt+osdtrt);
        psatwc=100*(wincty-wintrt)/(wintrt+osdtrt);
        psacws=100*osdcty/(wintrt+osdtrt);
        psrus=100*urswap/urcount;
   file summary;
   put 'percent of all households swapped:        ' pahs 5.2;
   put 'percent of subjected racial uniq swapped  ' psrus 5.2;
   put 'percent selected racial uniq unswapped:   ' pruu 5.2;
   put 'percent selected subjected unswapped:     ' puu 5.2;
   put 'percent swaps within tract:               ' pswt 5.2;
   put 'percent swaps across tract within county: ' psatwc 5.2;
   put 'percent swaps across county within state: ' psacws 5.2;
        put;
   file stat;
   put wintrt wincty urcount stract scounty nopair nopairu osdcty osdtrt
       nuwincty nuwintrt nuosdcty nuosdtrt puu pswt pahs pruu psacws psatwc
       psrus urswap;
%mend georeprt;

%macro swap1;

*main routine, determines which of the selected can be swapped, outputs the
list of pairs, lots of counters to keep track of which selection criteria is
in force and what the geographic level of the swap is, outputs the state-block
uniques (1) and the failures (2).  the counters get dumped to a flat file
which gets read into a sas set later. the output is split, in a prior version
there was additional processing; 

data
     swapped(drop=pcontrol scontrol wintrt wincty urcount stract urswap
scounty nopair nopairu osdcty osdtrt nuwincty nuwintrt nuosdcty nuosdtrt
puu pswt pahs pruu psacws psatwc psrus inkey select sinkey skey flag x y
tflag key okey)

     uswapped(drop=pcontrol scontrol wintrt wincty urcount stract urswap
scounty nopair nopairu osdcty osdtrt nuwincty nuwintrt nuosdcty nuosdtrt
puu pswt pahs pruu psacws psatwc psrus inkey select sinkey skey flag x y
tflag newcont cselect key okey)

     me.&state.unswp1(keep=control uniqkey cselect okey)
     me.&state.unswp2(keep=control uniqkey cselect okey);

     retain sflag (0);        *count of records to go in swap sequence;
     retain last last2 (0 0); *for finish-need to process last record;
     retain pcontrol;         *prior records control number in sort;

     retain nopair nopairu urcount urswap (0 0 0 0 0);
     retain wincty wintrt osdcty osdtrt (0 0 0 0);    *geo counters;
     retain nuwincty nuwintrt nuosdcty nuosdtrt (0 0 0 0);

     length nopair nopairu urcount wincty wintrt osdcty osdtrt
            nuwincty nuwintrt nuosdcty nuosdtrt puu pswt pahs pruu
            psacws psatwc 5;
     length sflag tflag 4;
     length flag last2 last x y 3;
     length pcontrol $12.;
	 length newcont $12.;

     if last=0 then do;                      *peak ahead;
        set select2(firstobs=2
                    rename=(control=scontrol county=scounty tract=stract
                            key2=skey)
                    keep=control county tract pseudo_county pseudo_tract select inkey sinkey key2)
                    end=last;
     end;
     set select2(firstobs=1 drop=inkey sinkey
                 rename=(select=cselect key=okey key2=key)) end=last2;
     by key;

     if uniqkey=1 then urcount=urcount+1;
     if first.key and last.key then do;
         output me.&state.unswp1;             *write all unswappables;
     end;

*calculate a new swap sequence;

                      * inkey  is the count of records remaining in the key;
                      * sinkey is the count of selects remaining in the key;
                      * tflag temporarily holds the length of new sequence;
                      * sflag counts down the swap sequence;
                      * cselect indicates if currect record is selected(>0)
                        and how many selected records follow in a row-includes
                        current record in count;
                      * select indicates if the next record is selected and
                        how many selected records follow in a row-does not
                        count current record;
                      * flag indicates an early initiation of the sequence;

     tflag=0;
     flag=0;

*  special case when first record is selected, note the switch to cselect;

     if (_n_=1 and cselect ne 0) then do;
        x=mod(cselect,2);
        if skey=key then do;          *redundant;
          if x=0 then tflag=cselect;  *sequence even;
          else tflag=cselect+1;       *sequence odd;
        end;
        sflag=tflag;                  *trigger pairing this datastep;
        flag=1;                       *remember not to reassign sflag;
     end;

*  main selection of pairs;

     else if (cselect=0 and select>0) or (last.key and select>0) then do;
        x=mod(select,2);


*  1st case
if the number of selected records, in key and in sequence, is even there is no
problem: the number of pairs is the number of selected by 2.  If odd then a
nonselected record must be added to the sequence either before or after.  the
additional record must have the same key and preferably the same tract. at the
begining of a key (select inkey etc come from the peak ahead) or new sequence
abuts old sequence there are no choices: ;

        if last.key or sflag=1 then do;
           if x=0 then tflag=select;
           else if (x ne 0 and inkey ne select) then tflag=select+1;
           else if (x ne 0 and inkey eq select) then tflag=select-1;
                                                     *orphan;
        end;

* 2nd case
the default (third) choice is to take the current record to add to the
sequence coming up next however.  that it be >x is
required for the odd case;

        else if (tract ne stract and inkey-sinkey>x) then do;
           if x=0 then tflag=select;
           else tflag=select+1;
        end;

* default case;

        else do;
           if x=0 then do;
               tflag=select;                *sequence even;
           end;
           else do;
              if skey=key then do;          *redundant;
                tflag=select+1;             *sequence odd;
                sflag=tflag;                *trigger pairing this datastep;
                flag=1;                     *remember not to reassign sflag;
              end;
           end;
        end;
     end;           * end swap sequence calculation;

   * create output file listing pairs of records to be swapped ;

     y=mod(sflag,2);                        *do the writes when even;
     if sflag>0 and y=0 then do;
        if key ne skey then do;
            file summary mod;
            put 'bad select count, bad pair control: 'control' key ' key;
        end;
        newcont=scontrol;
        %countgeo;
        if uniqkey then urswap=urswap+1;
        file edited;
*        put control $char8.-r newcont $char8.-r;
        put control $char12.-r newcont $char12.-r;
        output swapped;
        sflag=sflag-1;
     end;
     else if y=1 then do;                   *y=1 --> sflag>0 (sflag odd);
        if uniqkey then urswap=urswap+1;
        newcont=pcontrol;
        output swapped;
        sflag=sflag-1;         *id switch for 2nd record (not used at present),
                               otherwise 2nd record looks unswapped in dataset;
     end;
     else if cselect>0 and sflag=0 then do;
           output uswapped;                 *tried to swap, couldnt;
           nopair=nopair+1;
           if uniqkey=1 then nopairu=nopairu+1;
           if not (first.key and last.key) then do;
              output me.&state.unswp2;         *if its not in unswappable file;
           end;
     end;
     else output uswapped;

     pcontrol=control;
     if tflag>0 and flag=0 then sflag=tflag;   *trigger for next data step;

     if last2=1 then do;
        file summary mod;
        %georeprt;
     end;
run;

data me.&state.stat;
   infile stat;
   input wintrt wincty urcount stract scounty nopair nopairu osdcty osdtrt
       nuwincty nuwintrt nuosdcty nuosdtrt puu pswt pahs pruu psacws psatwc
       psrus urswap;
run;
proc datasets nolist library=work;
    delete select2;
run;
proc printto print=debug;
run;
proc print data=swapped(obs=200);
    title 'first 200 records after swapping';
run;
data _null_;
   set me.&state.unswp1(obs=10);
   file 'temp.dat';
   put control 12.0 okey oky_&state.. ;
run;
data unswap1;
   infile 'temp.dat';
   input ten $ 9-9;
run;
proc print noobs data=unswap1;
   title '10 obs state-block uniques';
run;
data _null_;
   set me.&state.unswp2(obs=10);
   file 'temp.dat';
   put control 12.0 okey oky_&state.. ;
run;
data unswap2;
   infile 'temp.dat';
   input ten $ 9-9;
run;
proc print noobs data=unswap2;
   title '10 obs unswap2';
run;
proc printto;
run;

%HOWMANY(work,uswapped);
data _null_;
  file summary mod;
  title;
  put;
  put "&NUMOBS  unpartnered";
run;
%HOWMANY(work,swapped);
data _null_;
  file summary mod;
  title;
  put;
  put "&NUMOBS  swapped households";
run;

%HOWMANY(me,&state.unswp1);
data _null_;
  file summary mod;
  title;
  put;
  put "&NUMOBS  state-block uniques";
run;
%HOWMANY(me,&state.unswp2);
data _null_;
  file summary mod;
  title;
  put;
  put "&NUMOBS  swap failures";
run;

%mend swap1;

%macro merge2;

        * combine records that were not eligible for swapping
           with records that were (some of which were swapped) ;

data alldata(drop=sflag);
   set swapped  (in=n1
         keep=county tract block sflag prep uniqkey 
              unique pseudo_county pseudo_tract)
       dimpute  (in=n3
         keep=county tract block uniqkey)
       uswapped (in=n2
         keep=county tract block sflag prep uniqkey 
              unique);

   if n3 then do;
      prep=0;
      swapped=0;    *must be subjected to be swapped;   
      unique=0;
   end;   
   else do;
      if sflag>0 then swapped=1;
      else swapped=0;
   end;
run;
proc datasets nolist library=work;
    delete swapped uswapped dimpute;
run;

%HOWMANY(work,alldata);
data _null_;
  file summary mod;
  title;
  put;
  put "&NUMOBS  records in the reassembled data set (should match original)";
run;

%mend merge2;


%macro evaluate;                 *evaluate results ;

proc sort data=alldata out=alldata2;
by swapped unique ;
run;

proc datasets nolist library=work;
    delete alldata;
run;

proc summary data=alldata2;   *get counts and fractions of swapped and
                                unswapped uniques, etc. ;
class swapped unique ;
var swapped unique ;
output out=me.&state._class sum=swcount unicount  mean=swapmean unimean ;
run;

proc sort data=alldata2 out=alldata3;
by county tract swapped unique;
run;

proc datasets nolist library=work;
    delete alldata2;
run;

proc summary data=alldata3;    *get counts and fractions of swapped
                                      records by tract;
var swapped unique;
by county tract;
output out=me.&state._tract sum=swsum unsum mean=swmean unmean;
run;

proc sort data=alldata3 out=alldata4;
by swapped unique;
run;

proc datasets nolist library=work;
    delete alldata3;
run;

proc summary data=alldata4;  *get counts and fractions of swapped
                                      records and uniques by block size;
by;
var swapped unique;
output out=me.&state._block sum=b1swsum b1unsum mean=b1swmean b1unmean;
run;

proc printto print=summary;
run;
data top5;
  set me.&state._block(obs=5);
run;
proc print noobs data=top5;
   title 'fractions by blocksize';
   var _freq_ b1swmean b1unmean;
run;
proc printto;
run;

%mend evaluate;

     * read in the original data ;
     * add block imputation rate to data ;
     * get rid of imputed, make selection;
     * if too many are selected, cut it down;
     * perform the swapping ;
     * get final set of data --- some swapped and some not ;
     * evaluate results ;

%macro doit;      * run the program ;

%readin;
%impute;
%unique;
%estswap;
data _null_;
  x=0.001*&allhouse;
  call symput('tolrnc',put(x,10.));
run;                                    *if there is any reduction a set
					called dlist is created and merged 
 					into main set, otherwise procede with
 					no merge;
%if &deselect > &tolrnc %then %do;
   %reduce1;
   %if &deselect > &tolrnc %then %reduce2;
   %if &deselect > &tolrnc %then %reduce3;
   %deselect;
%end;
%else %dselect2;
%swap1;
%merge2;
%evaluate;
quit;
%mend doit;

%doit;            * run the program ;
